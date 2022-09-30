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
 *
 * Note that this implementation is highly experimental and subject to change or removal.
 *
 * @see <a href="https://zulip.com/api/real-time-events">https://zulip.com/api/real-time-events</a>
 */
public class EventPoller {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPoller.class.getName());

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
     * @param narrows  optional {@link Narrow} expressions to filter which message events are captured. E.g messages from a
     *                 specific stream
     */
    public EventPoller(ZulipHttpClient client, MessageEventListener listener, Narrow[] narrows) {
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
        if (status.equals(Status.STOPPED)) {
            status = Status.STARTING;
            LOGGER.info("EventPoller starting");

            RegisterEventQueueApiRequest createQueue = new RegisterEventQueueApiRequest(this.client, narrows);
            GetMessageEventsApiRequest getEvents = new GetMessageEventsApiRequest(this.client);

            queue = createQueue.execute();
            executor = Executors.newSingleThreadExecutor();

            executor.submit(new Runnable() {
                private long lastEventId = queue.getLastEventId();

                @Override
                public void run() {
                    while (status.equals(Status.STARTING) || status.equals(Status.STARTED)) {
                        try {
                            getEvents.withQueueId(queue.getQueueId());
                            getEvents.withLastEventId(lastEventId);

                            List<MessageEvent> messageEvents = getEvents.execute();
                            for (MessageEvent event : messageEvents) {
                                listener.onEvent(event.getMessage());
                            }

                            lastEventId = messageEvents.stream().max(Comparator.comparing(Event::getId))
                                    .get()
                                    .getId();

                            Thread.sleep(5000);
                        } catch (ZulipClientException e) {
                            EventPoller.LOGGER.warn("Error processing events - ", e);
                            if (e.getCode().equals("BAD_EVENT_QUEUE_ID")) {
                                // Queue may have been garbage collected so recreate it
                                try {
                                    queue = createQueue.execute();
                                } catch (ZulipClientException zulipClientException) {
                                    EventPoller.LOGGER.warn("Error recreating message queue - ", e);
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });

            status = Status.STARTED;
            LOGGER.info("EventPoller started");
        }
    }

    /**
     * Stops message polling.
     */
    public synchronized void stop() {
        if (status.equals(Status.STARTING) || status.equals(Status.STARTED)) {
            try {
                status = Status.STOPPING;
                executor.shutdown();
                DeleteEventQueueApiRequest deleteQueue = new DeleteEventQueueApiRequest(this.client, queue.getQueueId());
                LOGGER.info("EventPoller stopping");
                deleteQueue.execute();
            } catch (ZulipClientException e) {
                LOGGER.warn("Error deleting event queue - " + e);
            } finally {
                executor = null;
                status = Status.STOPPED;
                LOGGER.info("EventPoller stopped");
            }
        }
    }

    public boolean isStarted() {
        return status.equals(Status.STARTED);
    }

    private enum Status {
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }
}
