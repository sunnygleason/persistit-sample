package com.g414.persistit.samples;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleRegistration;
import com.g414.guice.lifecycle.LifecycleSupportBase;
import com.g414.persistit.Functional.Pair;
import com.g414.persistit.Template;
import com.g414.persistit.Template.ExchangeCallback;
import com.g414.persistit.Template.TransactionCallback;
import com.google.inject.Inject;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;

@Path("/1.0/c")
@Produces(MediaType.APPLICATION_JSON)
public class CounterServiceResource implements LifecycleRegistration {
	@Inject
	private Template<String, Long> dbt;

	@Inject
	private Persistit db;

	@Inject
	public void register(Lifecycle lifecycle) {
		lifecycle.register(new LifecycleSupportBase() {
			@Override
			public void shutdown() {
				try {
					db.close();
				} catch (PersistitException e) {
					throw new RuntimeException(e);
				}
			}
		});
	}

	@GET
	@Path("{key}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response getCounter(final @PathParam("key") String key)
			throws Exception {
		final Exchange exchange = getExchange();

		return dbt.withExchange(db, exchange, new ExchangeCallback<Response>() {
			@Override
			public Response run() {
				Pair<String, Long> row = dbt.load(exchange, key);

				if (row != null) {
					return Response.status(Status.OK)
							.entity(row.getValue().toString()).build();
				}

				return Response.status(Status.NOT_FOUND).entity("").build();
			}
		});
	}

	@POST
	@Path("{key}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response updateCounter(final @PathParam("key") String key,
			@QueryParam("i") Long increment) throws Exception {
		final Long i = (increment == null) ? 1L : increment;
		final Exchange exchange = getExchange();

		return dbt.withExchange(db, exchange, new ExchangeCallback<Response>() {
			@Override
			public Response run() {
				for (int retries = 0; retries < 20; retries++) {
					try {
						return dbt.inTransaction(db,
								new TransactionCallback<Response>() {
									@Override
									public Response inTransaction(
											Transaction txn) {
										Pair<String, Long> row = dbt.load(
												exchange, key);

										Long newValue = (row != null) ? row
												.getValue() + i : i;

										dbt.insertOrUpdate(exchange, key,
												newValue);

										return Response.status(Status.OK)
												.entity(newValue.toString())
												.build();
									}
								});
					} catch (PersistitException e) {
						// fall through to the delay below
					} catch (Exception e) {
						throw new RuntimeException(e);
					} finally {
						// this space intentionally left blank
					}

					try {
						Thread.sleep((retries << 6) + 10);
					} catch (InterruptedException e) {
						// this space intentionally left blank
					}
				}

				return Response.status(Status.SERVICE_UNAVAILABLE)
						.entity("update failed - please try again soon")
						.build();
			}
		});
	}

	@DELETE
	@Path("{key}")
	@Produces(MediaType.TEXT_PLAIN)
	public Response deleteData(final @PathParam("key") String key)
			throws Exception {
		final Exchange exchange = getExchange();

		return dbt.withExchange(db, exchange, new ExchangeCallback<Response>() {
			@Override
			public Response run() {
				boolean deleted = dbt.delete(exchange, key);

				if (deleted) {
					return Response.status(Status.OK).entity("").build();
				}

				return Response.status(Status.NOT_FOUND).entity("").build();
			}
		});
	}

	protected Exchange getExchange() throws PersistitException {
		return db.getExchange("the_volume", "the_tree", true);
	}
}
