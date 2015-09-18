package kvstore

import akka.actor.{Cancellable, Actor, ActorRef, Props}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import kvstore.Replicator.{Snapshot, SnapshotAck}

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class OperationTimeout(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  import scala.concurrent.duration._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // the persistent message we (tried to) send, but not yet persist acked
  var persistedSend = Map.empty[Long, (ActorRef, Option[Persist], Set[ActorRef])]

  var expectedSequence: Long = 0

  val persistence = context.actorOf(persistenceProps)

  override def preStart() = {
    arbiter ! Join
    context.system.scheduler.schedule(100 millis, 100 millis)(resendPersist())
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = LoggingReceive {
    case Insert(key, value, id) =>
      kv += (key -> value)
      triggerPersistPrimairy(key, Some(value), id)

    case Remove(key, id) =>
      kv -= key
      triggerPersistPrimairy(key, None, id)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Persisted(_, id) =>
      persistedSend.get(id).foreach {
        case (ref, _, set) =>
          persistedSend = persistedSend.updated(id, (ref, None, set))
      }
      checkFinished(id)

    case Replicas(replicas) =>
      replicas.filterNot(_ == self).foreach { ref =>
        val replicator = context.actorOf(Replicator.props(ref))
        secondaries += ref -> replicator
        replicators += replicator
      }

    case Replicated(key, id) =>
      persistedSend.get(id).foreach {
        case (ref, message, set) =>
          persistedSend = persistedSend.updated(id, (ref, message, set - sender()))
      }
      checkFinished(id)

    case OperationTimeout(id) =>
      persistedSend.get(id).foreach {
        case (ref, _, _) => ref ! OperationFailed(id)
      }
      persistedSend -= id
  }

  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOpt, seq) =>
      if (seq == expectedSequence) {
        valueOpt match {
          case Some(value) => kv += (key -> value)
          case None => kv -= key
        }
        expectedSequence += 1
        triggerPersist(key, valueOpt, seq)
      } else if (seq < expectedSequence) {
        sender ! SnapshotAck(key, seq)
      }

    case Persisted(key, seq) =>
      persistedSend.get(seq).foreach { _._1 ! SnapshotAck(key, seq) }
      persistedSend -= seq
  }

  def triggerPersistPrimairy(key: String, valueOpt: Option[String], seq: Long) = {
    triggerPersist(key, valueOpt, seq)
    replicators.foreach(_ ! Replicate(key, valueOpt, seq))
    context.system.scheduler.scheduleOnce(1 second, self, OperationTimeout(seq))
  }

  def triggerPersist(key: String, valueOpt: Option[String], seq: Long) = {
    val persistMessage = Persist(key, valueOpt, seq)
    persistedSend += seq ->(sender(), Some(persistMessage), replicators)
    persistence ! persistMessage
  }

  def checkFinished(id: Long) = {
    persistedSend.get(id).foreach {
      case (ref, None, s) if s.isEmpty =>
        ref ! OperationAck(id)
        persistedSend -= id
      case _ =>
    }
  }

  def resendPersist() = persistedSend.values.foreach {
    case (_, Some(m), _) => persistence ! m
    case _ =>
  }
}

