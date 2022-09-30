package com.github.jamesnetherton.zulip.client.api.event.request;

import static com.github.jamesnetherton.zulip.client.api.event.request.EventRequestConstants.REGISTER_QUEUE;

import com.github.jamesnetherton.zulip.client.api.core.ExecutableApiRequest;
import com.github.jamesnetherton.zulip.client.api.core.ZulipApiRequest;
import com.github.jamesnetherton.zulip.client.api.event.EventQueue;
import com.github.jamesnetherton.zulip.client.api.event.response.RegisterEventQueueApiResponse;
import com.github.jamesnetherton.zulip.client.api.narrow.Narrow;
import com.github.jamesnetherton.zulip.client.exception.ZulipClientException;
import com.github.jamesnetherton.zulip.client.http.ZulipHttpClient;

/**
 * Zulip API request builder for registering an event queue.
 *
 * @see <a href="https://zulip.com/api/register-queue">https://zulip.com/api/register-queue</a>
 */
public class RegisterEventQueueApiRequest extends ZulipApiRequest implements ExecutableApiRequest<EventQueue> {

    private static final String EVENT_TYPES = "event_types";
    private static final String ALL_PUBLIC_STREAMS = "all_public_streams";
    private static final String NARROW = "narrow";
    private static final String[] MONITORED_EVENTS = new String[] { "message" };

    /**
     * Constructs a {@link ZulipApiRequest}.
     *
     * @param client  The Zulip HTTP client
     * @param narrows optional {@link Narrow} expressions to filter which message events are captured. E.g messages
     *                from a specific stream
     */
    public RegisterEventQueueApiRequest(final ZulipHttpClient client, final Narrow... narrows) {
        super(client);
        this.putParamAsJsonString(EVENT_TYPES, MONITORED_EVENTS);
        this.putParamAsJsonString(ALL_PUBLIC_STREAMS, Boolean.TRUE);

        if (narrows.length > 0) {
            final String[][] stringNarrows = new String[1][narrows.length];
            for (int i = 0; i < narrows.length; i++) {
                stringNarrows[i] = new String[] { narrows[i].getOperator(), narrows[i].getOperand() };
            }

            this.putParamAsJsonString(NARROW, stringNarrows);
        }
    }

    /**
     * Executes the Zulip API request for registering an event queue.
     *
     * @return                      the created {@link EventQueue}
     * @throws ZulipClientException if the request was not successful
     */
    @Override
    public EventQueue execute() throws ZulipClientException {
        final RegisterEventQueueApiResponse response = this.client().post(REGISTER_QUEUE, this.getParams(),
                RegisterEventQueueApiResponse.class);
        return new EventQueue(response);
    }
}
