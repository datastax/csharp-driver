# Our policy regarding API changes and release versioning

The driver versions follow semantic versioning.

## Major releases

Example: `3.0.0`

Regarding `major` releases, any public component of the driver might be changed or removed although we will always try to avoid introducing significant changes that would make it significantly harder for an application to upgrade to the new `major` version.

## Minor releases

Example: `3.10.0`

For `minor` releases, it's a little more complicated because we don't want to be forced into bumping the `major` version every time we want to add a new client facing feature to the driver. For this reason, we group public interfaces of the driver in two categories. Here we refer to them as _mockable_ and _implementable_ but these names are here just to make it easier to explain them in this section.

### _Implementable_ Interfaces

These interfaces exist to allow client applications to provide customized behavior that will be executed by the driver. For these interfaces, the driver provides ways for the applications to plug them in. Some examples: policies such as `ILoadBalancingPolicy` can be plugged in through builder methods like `Builder.WithLoadBalancingPolicy`, `ITimestampGenerator` can be plugged in through `Builder.WithTimestampGenerator`.

**In `minor` releases, these interfaces (_Implementable_) will not contain any changes to existing methods. Additionally, they will not receive new methods.**

The reason why we commit to never add new methods to these interfaces in `minor` releases is due to the fact that these interfaces are meant to be implemented by client applications while the remaining interfaces are not meant to be implemented.

### _Mockable_ Interfaces

For the remaining policies, i.e., those who can not be plugged in to the driver, they exist to allow client applications to mock the driver in their test suites and inject those dependencies in the application when needed. These interfaces are usually the main entry points of the driver's public API with which client applications interact to execute requests. Some examples: `ISession`, `ICluster`, `IMapper`, `ICqlQueryAsyncClient`, `ICqlWriteAsyncClient`, `ICqlQueryClient`, `ICqlWriteClient`. These interfaces fall into this category (_Mockable_).

Other interfaces like `IStatement` and `ICqlBatch` can be passed as parameters to certain methods of the driver (`ISession.Execute(IStatement)`and `IMapper.Execute(ICqlBatch)`) but the client application will create these instances as part of the normal flow of execution through other methods:

- `IStatement` is created with the constructors of `SimpleStatement` and `BatchStatement` or with `PreparedStatement.Bind()` for instances of `BoundStatement`
- `ICqlBatch` is created with `IMapper.CreateBatch()`

Client applications shouldn't implement interfaces like `IStatement` and `ICqlBatch` and, therefore, these are also part of the _Mockable_ category.

**Users should expect new methods to be added to the interfaces that fall into this category (_Mockable_) in `minor` releases.**

We recommend users to use a mocking library that do not force applications to provide an implementation of every single method of an interface.

If you need to implement a wrapper class to provide functionality on top of the driver (like tracing), we recommend **composition** instead of **inheritance**, i.e., the wrapper class should have its own interface instead of implementing the driver interface.

## Patch releases

Example: `3.4.1`

These releases only contain bug fixes so they will never contain changes to the driver's public API.
