package com.github.jamesnetherton.zulip.client.api.event;

import com.github.jamesnetherton.zulip.client.api.event.request.DeleteEventQueueApiRequest;
import com.github.jamesnetherton.zulip.client.api.event.request.GetMessageEventsApiRequest;
import com.github.jamesnetherton.zulip.client.api.event.request.RegisterEventQueueApiRequest;
import com.github.jamesnetherton.zulip.client.api.narrow.Narrow;
import com.github.jamesnetherton.zulip.client.exception.ZulipClientException;
import com.github.jamesnetherton.zulip.client.http.ZulipHttpClient;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls Zulip for real-time events. At present this is limited to consuming new message events.
 * <p>
 * Note that this implementation is highly experimental and subject to change or removal.
 *
 * @see <a href="https://zulip.com/api/real-time-events">https://zulip.com/api/real-time-events</a>
 */
public class EventPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPoller.class.getName());

    private static final int SLEEP_INTERVAL = 1_000;

    private final MessageEventListener listener;
    private final ZulipHttpClient client;
    private final Narrow[] narrows;
    private volatile EventQueue queue;
    private volatile ExecutorService executor;
    private volatile Status status = Status.STOPPED;

    /**
     * Constructs a {@link EventPoller}.
     *
     * @param client   The Zulip HTTP client
     * @param listener The {@link MessageEventListener} to be invoked on each message event
     * @param narrows  optional {@link Narrow} expressions to filter which message events are captured. E.g messages
     *                 from a specific stream
     */
    public EventPoller(final ZulipHttpClient client, final MessageEventListener listener, final Narrow[] narrows) {
        this.client = client;
        this.listener = listener;
        this.narrows = narrows;
    }

    /**
     * Starts event message polling.
     *
     * @throws ZulipClientException if the event polling request was not successful
     */
    public synchronized void start() throws ZulipClientException {
        if (this.status.equals(Status.STOPPED)) {
            LOGGER.info("EventPoller starting");
            this.status = Status.STARTING;

            final RegisterEventQueueApiRequest createQueue = new RegisterEventQueueApiRequest(this.client, this.narrows);
            final GetMessageEventsApiRequest getEvents = new GetMessageEventsApiRequest(this.client);

            this.queue = createQueue.execute();
            this.executor = Executors.newSingleThreadExecutor();

            this.executor.submit(new Runnable() {
                private long lastEventId = EventPoller.this.queue.getLastEventId();

                @Override
                public void run() {
                    while (EventPoller.this.status.equals(Status.STARTING) || EventPoller.this.status.equals(Status.STARTED)) {
                        try {
                            EventPoller.LOGGER.debug("last_event_id: " + this.lastEventId);
                            getEvents.withQueueId(EventPoller.this.queue.getQueueId());
                            getEvents.withLastEventId(this.lastEventId);

                            final List<MessageEvent> messageEvents = getEvents.execute();
                            for (final MessageEvent event : messageEvents) {
                                EventPoller.this.listener.onEvent(event.getMessage());
                            }

                            this.lastEventId = messageEvents.stream().max(Comparator.comparing(Event::getId))
                                    .get()
                                    .getId();

                            Thread.sleep(EventPoller.SLEEP_INTERVAL);
                        } catch (final ZulipClientException e) {
                            EventPoller.LOGGER.warn("Error processing events - ", e);
                            if (e.getCode().equals("BAD_EVENT_QUEUE_ID")) {
                                // Queue may have been garbage collected so recreate it
                                try {
                                    EventPoller.this.queue = createQueue.execute();
                                } catch (final ZulipClientException zulipClientException) {
                                    EventPoller.LOGGER.warn("Error recreating message queue - ", e);
                                }
                            }
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });

            LOGGER.info("EventPoller started");
            this.status = Status.STARTED;
        }
    }

    /**
     * Stops message polling.
     */
    public synchronized void stop() {
        if (this.status.equals(Status.STARTING) || this.status.equals(Status.STARTED)) {
            try {
                LOGGER.info("EventPoller stopping");
                this.status = Status.STOPPING;
                this.executor.shutdown();
                final DeleteEventQueueApiRequest deleteQueue = new DeleteEventQueueApiRequest(this.client,
                        this.queue.getQueueId());
                deleteQueue.execute();
            } catch (final ZulipClientException e) {
                LOGGER.warn("Error deleting event queue - " + e);
            } finally {
                LOGGER.info("EventPoller stopped");
                this.executor = null;
                this.status = Status.STOPPED;
            }
        }
    }

    public boolean isStarted() {
        return this.status.equals(Status.STARTED);
    }

    private enum Status {
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }
}
