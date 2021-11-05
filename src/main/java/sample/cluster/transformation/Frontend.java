package sample.cluster.transformation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.lang.Math;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;

//#frontend
public class Frontend extends AbstractBehavior<Frontend.Event> {

  interface Event {}
  private enum Tick implements Event {
    INSTANCE
  }
  private static final class WorkersUpdated implements Event {
    public final Set<ActorRef<Worker.TransformText>> newWorkers;
    public WorkersUpdated(Set<ActorRef<Worker.TransformText>> newWorkers) {
      this.newWorkers = newWorkers;
    }
  }
  private static final class TransformCompleted implements Event {
    public final String originalText;
    public final String transformedText;
    public TransformCompleted(String originalText, String transformedText) {
      this.originalText = originalText;
      this.transformedText = transformedText;
    }
  }
  private static final class JobFailed implements Event {
    public final String why;
    public final String text;
    public JobFailed(String why, String text) {
      this.why = why;
      this.text = text;
    }
  }

  public static Behavior<Event> create() {
    return Behaviors.setup(context ->
        Behaviors.withTimers(timers ->
          new Frontend(context, timers)
        )
    );
  }

  private final List<ActorRef<Worker.TransformText>> workers = new ArrayList<>();
  private int jobCounter = 0;
  private static int UPPER = 1000;
  private static int LOWER = 10;

  private Frontend(ActorContext<Event> context, TimerScheduler<Event> timers) {
    super(context);
    ActorRef<Receptionist.Listing> subscriptionAdapter =
        context.messageAdapter(Receptionist.Listing.class, listing ->
          new WorkersUpdated(listing.getServiceInstances(Worker.WORKER_SERVICE_KEY)));
    context.getSystem().receptionist().tell(Receptionist.subscribe(Worker.WORKER_SERVICE_KEY, subscriptionAdapter));

    timers.startTimerWithFixedDelay(Tick.INSTANCE, Tick.INSTANCE, Duration.ofSeconds(15));
  }

  @Override
  public Receive<Event> createReceive() {
    return newReceiveBuilder()
        .onMessage(WorkersUpdated.class, this::onWorkersUpdated)
        .onMessage(TransformCompleted.class, this::onTransformCompleted)
        .onMessage(JobFailed.class, this::onJobFailed)
        .onMessageEquals(Tick.INSTANCE, this::onTick)
        .build();
  }


  private Behavior<Event> onTransformCompleted(TransformCompleted event) {
    getContext().getLog().info("{} is completed. Details: {}", event.originalText, event.transformedText);
    return this;
  }

  private Behavior<Event> onJobFailed(JobFailed event) {
    getContext().getLog().warn("{} is failed. Caused by: {}", event.text, event.why);
    return this;
  }
  
  private int createJobCardID() {
      int jobID = (int) (Math.random() * (UPPER - LOWER)) + LOWER;
      return jobID;
  }

  private Behavior<Event> onTick() {
    if (workers.isEmpty()) {
      getContext().getLog().warn("Got a job request but no workers available, not sending any work");
    } else {
      // how much time can pass before we consider a request failed
      Duration timeout = Duration.ofSeconds(10);
      ActorRef<Worker.TransformText> selectedWorker = workers.get(jobCounter % workers.size());
      String text = "job-card-" + this.createJobCardID();
      getContext().getLog().info("Sending {} to {}", text, selectedWorker);
      getContext().ask(
          Worker.TextTransformed.class,
          selectedWorker,
          timeout,
          responseRef -> new Worker.TransformText(text, "workerRobot", responseRef),
          (response, failure) -> {
            if (response != null) {
              return new TransformCompleted(text, response.text);
            } else {
              return new JobFailed("Processing timed out", text);
            }
          }
      );
      jobCounter++;
    }
    return this;
  }

  private Behavior<Event> onWorkersUpdated(WorkersUpdated event) {
    workers.clear();
    workers.addAll(event.newWorkers);
    getContext().getLog().info("List of services registered with the receptionist changed: {}", event.newWorkers);
    return this;
  }
}
//#frontend