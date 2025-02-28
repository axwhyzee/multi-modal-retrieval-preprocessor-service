from dependency_injector import containers, providers
from event_core.adapters.services.mapping import RedisMapper
from event_core.adapters.services.storage import StorageAPIClient

MODULES = ("app", "__main__")


class DIContainer(containers.DeclarativeContainer):
    storage_client = providers.Singleton(StorageAPIClient)
    mapper = providers.Singleton(RedisMapper)


def bootstrap() -> None:
    container = DIContainer()
    container.wire(modules=MODULES)
