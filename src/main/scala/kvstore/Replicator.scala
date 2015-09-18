package kvstore

import akka.actor.{Actor, ActorRef, Props}

import scala.concurrent.duration._

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  override def preStart() = {
    context.system.scheduler.schedule(200 millis, 100 millis)(resend())
  }

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case m@Replicate(key, valueOpt, id) =>
      val seq = nextSeq
      sendSnapshot(key, valueOpt, seq)
      acks += seq ->(sender(), m)
    case SnapshotAck(key, seq) =>
      acks.get(seq).foreach { pair =>
        val (originalSender, replicate) = pair
        originalSender ! Replicated(replicate.key, replicate.id)
      }
      acks -= seq
  }

  def sendSnapshot(key: String, valueOpt: Option[String], seq: Long) = {
    replica ! Snapshot(key, valueOpt, seq)
  }

  def resend() = {
    acks.foreach {

      case (seq, (_, r)) => sendSnapshot(r.key, r.valueOption, seq)
    }
  }
}
