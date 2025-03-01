from dependency_injector import containers, providers
from event_core.adapters.services.meta import RedisMetaMapping
from event_core.adapters.services.storage import StorageAPIClient

MODULES = ("app", "__main__")


class DIContainer(containers.DeclarativeContainer):
    storage = providers.Singleton(StorageAPIClient)
    meta = providers.Singleton(RedisMetaMapping)


def bootstrap() -> None:
    container = DIContainer()
    container.wire(modules=MODULES)
