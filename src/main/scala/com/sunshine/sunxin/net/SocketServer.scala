package com.sunshine.sunxin.net

import akka.actor._
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress

class SocketServer(handlerClass: Class[_], localAddress: InetSocketAddress) extends Actor with ActorLogging {
  import Tcp._
  import context.system

  //当server关闭时关闭所有的子Actor
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {
    IO(Tcp) ! Bind(self, localAddress)
  }

  override def postRestart(thr: Throwable): Unit = context stop self

  def receive = {
    case Bound(localAddress) =>
      log.info(s"Listening on port ${localAddress.getPort}")

    case CommandFailed(Bind(_, local, _, _, _)) =>
      log.warning(s"Cannot bind to [$local]")
      context stop self

    case Connected(remote, local) =>
      log.info(s"Received connection from $remote")
      val handler = context.actorOf(Props(classOf[SocketHandler], sender(), remote, handlerClass))
      sender() ! Register(handler, keepOpenOnPeerClosed = true)
  }
}

//SocketHandler使用nack方式，不等对端ack就一直写入数据，仅当写入失败时才重传数据
class SocketHandler(connection: ActorRef, remote: InetSocketAddress, handlerClass: Class[_])
  extends Actor with ActorLogging {

  import Tcp._
  import SocketHandler._

  context watch connection

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {
    val codec = context.actorOf(Props(handlerClass, self))
    //TODO:告知编解码器，让编解码器watch handler
  }

  def receive = writing

  def writing: Receive  = {
    case Received(data) =>
      connection ! Write(data, Ack(currentOffset))
      buffer(data)

    case Ack(ack) =>
      acknowledge(ack)

    case CommandFailed(Write(_, Ack(ack))) =>
      connection ! ResumeWriting
      context become buffering(ack)

    case PeerClosed =>
      if (storage.isEmpty) context stop self
      else context become closing
  }

  def buffering(nack: Int): Receive = {
    var toAck = 10
    var peerClosed = false

    {
      case Received(data)  => buffer(data)
      case WritingResumed  => writeFirst()
      case PeerClosed      => peerClosed = true
      case Ack(ack) if ack < nack => acknowledge(ack)
      case Ack(ack) =>
        acknowledge(ack)
        if (storage.nonEmpty) {
          if (toAck > 0) {
            writeFirst()
            toAck -= 1
          } else {
            writeAll()
            context become (if (peerClosed) closing else  writing)
          }
        } else if (peerClosed) context stop self
        else  context become writing
    }
  }

  def closing: Receive = {
    case CommandFailed(_: Write) =>
      connection ! ResumeWriting
      context.become({
        case WritingResumed =>
          writeAll()
          context.unbecome()

        case ack: Int => acknowledge(ack)
      }, discardOld = false)

    case Ack(ack) =>
      acknowledge(ack)
      if (storage.isEmpty) context stop self
  }

  override def postStop(): Unit = {
    log.info(s"Transferred $transferred bytes from/to [$remote]")
  }

  //storageOffset是已经发送完的数据，也是写入缓存数据头的偏移
  private var storageOffset = 0
  //storage缓存数据
  private var storage = Vector.empty[ByteString]
  //stored缓存字节数
  private var stored = 0L
  //发送字节数
  private var transferred = 0L

  val maxStored = 100000000L
  val highWatermark = maxStored * 5 / 10
  val lowWatermark = maxStored * 3 / 10
  private var suspended = false

  private def currentOffset = storageOffset + storage.size

  private def buffer(data: ByteString): Unit = {
    storage :+= data
    stored += data.size

    if (stored > maxStored) {
      log.warning(s"Drop connection to [$remote] (buffer overrun)")
      context stop self
    } else if (stored > highWatermark) {
      log.debug(s"Suspending reading at $currentOffset")
      connection ! SuspendReading
      suspended = true
    }
  }

  private def acknowledge(ack: Int): Unit = {
    require(ack == storageOffset, s"Received ack $ack at $storageOffset")
    require(storage.nonEmpty, s"Storage was empty at ack $ack")

    val size = storage(0).size
    stored -= size
    transferred += size

    storageOffset += 1
    storage = storage drop 1

    if (suspended && stored < lowWatermark) {
      log.debug("Resuming reading")
      connection ! ResumeReading
      suspended = false
    }
  }

  private def writeFirst(): Unit = {
    connection ! Write(storage(0), Ack(storageOffset))
  }

  private def writeAll(): Unit = {
    for ((data, i) <- storage.zipWithIndex) {
      connection ! Write(data, Ack(storageOffset + i))
    }
  }
}

object SocketHandler {
  import Tcp._
  final case class Ack(offset: Int) extends Event

  def props(connection: ActorRef, remote: InetSocketAddress): Props =
    Props(classOf[SocketHandler], connection, remote)
}
