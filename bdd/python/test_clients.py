from pathlib import Path

from pytest_bdd import scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "clients.feature"))


@when("I list every connected client")
def list_every_client(world):
    world.clients = world.run(lambda: world.laser.client_metadata_all())


@then("the listing holds an authenticated connection")
def listing_holds_an_authenticated_connection(world):
    users = [client["user_id"] for client in world.clients if client.get("user_id") is not None]
    assert users, "a listed connection is authenticated"
    world.client_user = users[0]


@when("I list every connected client of that user")
def list_every_client_of_that_user(world):
    assert world.client_user is not None, "a user was listed"
    world.clients = world.run(lambda: world.laser.client_metadata_all(principal=world.client_user))


@then("every listed connection belongs to that user")
def every_listed_connection_belongs_to_that_user(world):
    assert world.clients, "the user's own connection is listed"
    assert all(client.get("user_id") == world.client_user for client in world.clients)
