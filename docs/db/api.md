# API Reference: mint.db

Async shown throughout (`mint.db.asynk`) — `mint.db.sync` mirrors every
signature with `async`/`await` removed and `Session` in place of
`AsyncSession`; it is not documented separately below since there is no
behavioral difference to distinguish (see the [Usage guide](usage.md#sync-vs-async)).

## Database

::: mint.db.asynk.Database

## Repositories

::: mint.db.asynk.RepositoryBase

::: mint.db.asynk.EntityRepository

## Mixins

::: mint.db.asynk.SoftDeleteMixin

::: mint.db.asynk.ResourceOwnerMixin

::: mint.db.asynk.IOwner

## Unit of Work

::: mint.db.asynk.UnitOfWork

## Materialized Views

::: mint.db.asynk.MaterializedViewRepository

## Models and mixins (`mint.db.models`)

::: mint.db.models.Base

::: mint.db.models.BaseWithUUID

::: mint.db.models.BaseWithIntID

::: mint.db.models.BaseWithStrID

::: mint.db.models.AuditMixin

::: mint.db.models.OwnerMixin

::: mint.db.models.IsDeletedMixin

## Typed results (`mint.db.typedefs`)

::: mint.db.typedefs.PaginatedResult

## Exceptions (`mint.db.exc`)

::: mint.db.exc.RepositoryError

::: mint.db.exc.SessionNotInitializedError

::: mint.db.exc.NotFoundError

::: mint.db.exc.OperationalError

::: mint.db.exc.ConfigError

::: mint.db.exc.DBSchemaNotSetError
