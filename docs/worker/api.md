# API Reference: mint.worker

## Worker

::: mint.worker.worker.Worker

::: mint.worker.worker.WorkerBinding

## App

::: mint.worker.app.WorkerApp

## Coordinator (centralized mode)

::: mint.worker.coordinator.Coordinator

::: mint.worker.coordinator.InFlightNode

## Canvas engine

::: mint.worker.canvas.engine.CanvasEngine

::: mint.worker.canvas.dispatch.Dispatch

## Canvas builder DSL

::: mint.worker.canvas.builder.Node

::: mint.worker.canvas.builder.Chain

::: mint.worker.canvas.builder.Chord

## Canvas models

::: mint.worker.canvas.models.TaskNode

::: mint.worker.canvas.models.ChainNode

::: mint.worker.canvas.models.GroupNode

::: mint.worker.canvas.models.NodeOutcome

::: mint.worker.canvas.models.ChildResult

::: mint.worker.canvas.models.FanIn

::: mint.worker.canvas.models.ErrorInfo

## Envelope

::: mint.worker.envelope.Envelope

## Enums (`mint.worker.enums`)

::: mint.worker.enums.NodeType

::: mint.worker.enums.NodeStatus

::: mint.worker.enums.CanvasStatus

::: mint.worker.enums.ErrorPolicy

::: mint.worker.enums.DeliveryGuarantee

## Brokers (`mint.worker.brokers`)

::: mint.worker.brokers.interface.IBroker

::: mint.worker.brokers.interface.Delivery

::: mint.worker.brokers.memory.MemoryBroker

::: mint.worker.brokers.rabbitmq.RabbitMQBroker

::: mint.worker.brokers.redis.RedisBroker

::: mint.worker.brokers.nats.NatsBroker

::: mint.worker.brokers.kafka.KafkaBroker

## Stores (`mint.worker.stores`)

::: mint.worker.stores.interface.ICanvasStore

::: mint.worker.stores.interface.GroupProgress

::: mint.worker.stores.memory.MemoryCanvasStore

::: mint.worker.stores.redis.RedisCanvasStore

## Executors (`mint.worker.executors`)

::: mint.worker.executors.interface.ITaskExecutor

::: mint.worker.executors.interface.IClosableExecutor

::: mint.worker.executors.inline.InlineExecutor

::: mint.worker.executors.thread_pool.ThreadPoolExecutor

::: mint.worker.executors.process_pool.ProcessPoolExecutor

::: mint.worker.executors.grpc.GRPCExecutor

::: mint.worker.executors.amqp_rpc.AMQPRPCExecutor

::: mint.worker.executors.amqp_rpc.AMQPRPCConfig

## Exceptions (`mint.worker.exc`)

::: mint.worker.exc.WorkerError

::: mint.worker.exc.NodeNotFoundError

::: mint.worker.exc.ParentNotFoundError

::: mint.worker.exc.CallbackNotFoundError

::: mint.worker.exc.InvalidParentTypeError

::: mint.worker.exc.DuplicateNodeIdError

::: mint.worker.exc.MissingInputError

::: mint.worker.exc.ConflictingErrorPolicyError

::: mint.worker.exc.CanvasCycleError

::: mint.worker.exc.ResultTooLargeError

::: mint.worker.exc.WorkerNotBoundError

::: mint.worker.exc.MissingWorkerConfigError

::: mint.worker.exc.DuplicateTopicError

::: mint.worker.exc.AppAlreadyRunningError

::: mint.worker.exc.CoordinatorAlreadyRunningError

::: mint.worker.exc.UnpicklableTaskError

::: mint.worker.exc.RemoteMethodNotFoundError

::: mint.worker.exc.RemoteCallTimeoutError
