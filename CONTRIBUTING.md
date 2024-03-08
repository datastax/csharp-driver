# Contributing

When contributing to this repository, please first discuss the changes you wish to make via [JIRA issues][jira] or [mailing list threads][ml].

## Automated checks - Code Analyzers

The CI builds run a couple of code analyzers: [FxCop][fxcop] and [StyleCop][stylecop]. At this time, the severity for a lot of warnings and errors is set to `suggestion` because we are still in the process of fixing them in the entire codebase. Progress on this is tracked on [CSHARP-909][CSHARP909].

The setting that controls whether code analysis runs during the build process is set on the [Directory.Build.props](./src/Directory.Build.props) file:

```xml
<Project>
    <ItemGroup Condition="'$(RunCodeAnalyzers)' == 'True'">
        <PackageReference Include="Microsoft.CodeAnalysis.FxCopAnalyzers" Version="3.0.0">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
        <PackageReference Include="StyleCop.Analyzers" Version="1.1.118">
            <PrivateAssets>all</PrivateAssets>
            <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
        </PackageReference>
    </ItemGroup>
</Project>
```

As you can see, you can set the `RunCodeAnalyzers` environment variable or you can manually edit this file locally (don't commit this change).

### StyleCop

We follow the style guide enforced by StyleCop but we changed a couple of the default rules:

- [SA1101]: in this project we don't use the `this.` prefix for local members so we disabled [SA1101] and enabled [SX1101] instead;
- [SA1309]: in this project we use the `_` prefix for field names so we disabled [SA1309] and enabled [SX1309] instead.

If you're not familiar with the rules enforced by StyleCop, you can read them [here][stylecoprules].

### XML docs

All elements of the public API must be documented. For "internal" elements, documentation is optional, but in no way discouraged: it's generally a good idea to have a class-level comment that explains where the component fits in the architecture, and anything else that you feel is important.

You don't need to document every parameter or return type if they are obvious.

Driver users coding in their IDE should find the right documentation at the right time. Try to think of how they will come into contact with the class. For example, if a type is constructed with a builder, each builder method should probably explain what the default is when you don't call it.

Avoid using too many links, they can make comments harder to read, especially in the IDE. Link to a type the first time it's mentioned, then use a text description ("this registry"...). Don't link to a class in its own documentation. Don't link to types that appear right below in the documented item's signature.

## Logs

We use the .NET Tracing API and `Microsoft.Extensions.Logging`; loggers are declared like this:

```csharp
private static readonly Logger Logger = new Logger(typeof(BatchStatement));
```

Logs are intended for two personae:

- Ops who manage the application in production.
- Developers (maybe you) who debug a particular issue.

The first 3 log levels are for ops:

- `Error`: something that renders the driver -- or a part of it -- completely unusable. An action is required to fix it: bouncing the client, applying a patch, etc.
- `Warning`: something that the driver can recover from automatically, but indicates a configuration or programming error that should be addressed. For example: the driver connected successfully, but one of the contact points in the configuration was malformed; the same prepared statement is being prepared multiple time by the application code.
- `Info`: something that is part of the normal operation of the driver, but might be useful to know for an operator. For example: the driver has initialized successfully and is ready to process queries; an optional dependency was detected in the classpath and activated an enhanced feature.

Do not log errors that are rethrown to the client (such as the error that you're going to complete a request with). This is annoying for ops because they see a lot of stack traces that require no actual action on their part, because they're already handled by application code.

The last log level, i.e. `Verbose`, is for developers, to help follow what the driver is doing from a "black box" perspective (think about debugging an issue remotely, and all you have are the logs):

- `Verbose`: everything else. For example, node state changes, control connection activity, things that happen on each user request, etc.

When you add or review new code, take a moment to run the tests in `Verbose` mode and check if the
output looks good.

### Never assume a specific format for `ToString()`

Only use `ToString()` for debug logs or exception messages, and always assume that its format is unspecified and can change at any time.
  
If you need a specific string representation for a class, make it a dedicated method with a documented format, for example `ToCqlLiteral`. Otherwise it's too easy to lose track of the intended usage and break things: for example, someone modifies your `ToString()` method to make their logs prettier, but unintentionally breaks the script export feature that expected it to produce CQL literals. `ToString()` can delegate to `ToCqlLiteral()` if that is appropriate for logs.

## Unit tests

They live in the `Cassandra.Tests` project under the same folder/namespace (usually) as the code they are testing. They should be fast and not start any external process. They usually target one specific component and mock the rest of the driver context.

## Integration tests

They live in the `Cassandra.IntegrationTests` project and exercise the whole driver stack against an external
process, which can be either one of:

- [Simulacron][simulacronrepo]: simulates Cassandra nodes on loopback addresses; your test must "prime" data, i.e. tell the nodes what results to return for pre-determined queries.
- [CCM][ccmrepo]: launches actual Cassandra nodes locally.

In both cases, the `CASSANDRA_VERSION` environment variable determines which server version is used to create the Cassandra nodes.

## Building the driver and running tests

DataStax C# drivers target .NET Framework 4.5.2 and .NET Standard 2.0. The test projects target .NET Framework 4.6.2, 4.7.2, 4.8.1 and .NET 6, 7 and 8. To run the code analyzers you need the .NET 8 SDK.

### Prerequisites

- [.NET 8 SDK][dotnetsdk]

### IDE Support

You can build and run tests on Visual Studio 2022+ and JetBrains Rider by opening the solution file `Cassandra.sln` with any of those applications.

To run the code analyzers you need to update these IDEs to a version that supports `.editorconfig` because the code analysis settings are set on that file. In the case of Visual Studio, old versions don't support nested `.editorconfig` files and we have a couple of these on the codebase.

You can also use Visual Studio Code although the repository doesn't contain the configuration for it.

### Building

```bash
dotnet restore src
dotnet build src/Cassandra.sln
```

On Windows, the command `dotnet build src/Cassandra.sln` should succeed while on macOS / Linux it may fail due to the lack of support for .NET Framework builds on non-Windows platforms. In these environments you need to specify a .NET target framework in order to successfully build the project.

You can build specific projects against specific target frameworks on any platform like this:

```bash
dotnet build src/Cassandra/Cassandra.csproj -f netstandard2.0
dotnet build src/Cassandra.Tests/Cassandra.Tests.csproj -f net8
dotnet build src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -f net8
```

Alternatively you can set the `BuildCoreOnly` environment variable which will cause the projects to support .NET Core / .NET Standard targets only (you can see the conditions on the `.csproj` files).

### Running Unit Tests

```bash
dotnet test src/Cassandra.Tests/Cassandra.Tests.csproj -f net8
```

The target frameworks supported by the test projects are `net8` and `net481` (by default). If you set the `BuildAllTargets` environment variable, the test projects will support these targets:

- `net462`
- `net472`
- `net481`
- `net6` 
- `net7`(not LTS, might be removed at some point)
- `net8`

Running the unit tests for a single target should take no more than 5 minutes (usually less):

```bash
dotnet test src/Cassandra.Tests/Cassandra.Tests.csproj -c Release -f net8 -l "console;verbosity=detailed"
```

### Running Integration Tests

#### Required tools

There are a couple of tools that you need to run the integration tests: Simulacron and CCM.

##### CCM

To run the integration tests you need [ccm][ccmrepo] on your machine and make the ccm commands accessible from command line path. You should be able to run `> cmd.exe /c ccm help` using command line on Windows or `$ /usr/local/bin/ccm help` on Linux / macOS.

##### Simulacron

To run most of the integration tests you also need [simulacron][simulacronrepo]:

1. Download the latest jar file [here][simulacronreleases].
2. Set `SIMULACRON_PATH` environment variable to the path of the jar file you downloaded in the previous step.

Simulacron relies on loopback aliases to simulate multiple nodes. On Linux or Windows, you shouldn't have anything to do. On MacOS, run this script:

```bash
#!/bin/bash
for sub in {0..4}; do
    echo "Opening for 127.0.$sub"
    for i in {0..255}; do sudo ifconfig lo0 alias 127.0.$sub.$i up; done
done
```

Note that this is known to cause temporary increased CPU usage in OS X initially while mDNSResponder acclimates itself to the presence of added IP addresses. This lasts several minutes. Also, this does not survive reboots.

#### Test Categories

Integration tests are tagged with one or more categories. You can see the list of categories on the [TestCategory](./src/Cassandra.Tests/TestCategory.cs) static class under the unit test project (`Cassandra.Tests`).

#### Running each integration test suite

CCM tests usually take a bit longer to run so if you want a quick validation you might prefer to run the simulacron tests only. You can do this by running the tests that don't have the `realcluster` or `realclusterlong` categories:

```bash
dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -c Release -f net8 --filter "(TestCategory!=realcluster)&(TestCategory!=realclusterlong)" -l "console;verbosity=detailed"
```

This currently takes less than 10 minutes.

If you get this error: `Simulacron start error: java.net.BindException: Address already in use: bind` then you need to manually kill the `java` process. This happens when the test runner is interrupted (it doesn't terminate the simulacron process).

To run the integration tests suite that the **per commit** schedule builds use on Appveyor and Jenkins, do this:

```bash
dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -c Release -f net8 --filter "(TestCategory!=realclusterlong)" -l "console;verbosity=detailed"
```

This test suite contains all simulacron tests and most ccm tests. This currently takes less than 30 minutes for Apache Cassandra 3.11.x (which is the current default server version). You can change this by setting the `CASSANDRA_VERSION` environment variable or changing the default value of the `TestClusterManager.CassandraVersionString` property (don't commit this change).

To run all the integration tests (those that run on the **weekly** and **nightly** schedules), don't specify any filter:

```bash
dotnet test src/Cassandra.IntegrationTests/Cassandra.IntegrationTests.csproj -c Release -f net8 -l "console;verbosity=detailed"
```

This currently takes less than 45 minutes for Apache Cassandra 3.11.x.

## License headers

The code analysis that runs on CI builds will return errors if the license headers are missing.

## Commits

Keep your changes **focused**. Each commit should have a single, clear purpose expressed in its message.

Resist the urge to "fix" cosmetic issues (add/remove blank lines, move methods, etc.) in existing code. This adds cognitive load for reviewers, who have to figure out which changes are relevant to the actual issue. If you see legitimate issues, like typos, address them in a separate commit (it's fine to group multiple typo fixes in a single commit).

Isolate trivial refactorings into separate commits. For example, a method rename that affects dozens of call sites can be reviewed in a few seconds, but if it's part of a larger diff it gets mixed up with more complex changes (that might affect the same lines), and reviewers have to check every line.

Commit message subjects start with a capital letter, use the imperative form and do **not** end with a period:

- correct: "Add test for CQL request handler"
- incorrect: "~~Added test for CQL request handler~~"
- incorrect: "~~New test for CQL request handler~~"

Avoid catch-all messages like "Minor cleanup", "Various fixes", etc. They don't provide any useful information to reviewers, and might be a sign that your commit contains unrelated changes.

We don't enforce a particular subject line length limit, but try to keep it short.

You can add more details after the subject line, separated by a blank line.

## Pull requests

Like commits, pull requests should be focused on a single, clearly stated goal.

Don't base a pull request onto another one, it's too complicated to follow two branches that evolve at the same time. If a ticket depends on another, wait for the first one to be merged.

If you have to address feedback, avoid rewriting the history (e.g. squashing or amending commits). This makes the reviewers' job harder, because they have to re-read the full diff and figure out where your new changes are. Instead, push a new commit on top of the existing history; it will be squashed later when the PR gets merged.

If you need new stuff from the base branch, it's fine to rebase and force-push, as long as you don't rewrite the history. Just give a heads up to the reviewers beforehand. Avoid pushing merge commits to a pull request.

[ml]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
[jira]: https://datastax-oss.atlassian.net/browse/CSHARP
[CSHARP909]: https://datastax-oss.atlassian.net/browse/CSHARP-909
[fxcop]: https://github.com/dotnet/roslyn-analyzers#microsoftcodeanalysisfxcopanalyzers
[stylecop]: https://github.com/DotNetAnalyzers/StyleCopAnalyzers#stylecop-analyzers-for-the-net-compiler-platform
[SX1309]: https://github.com/DotNetAnalyzers/StyleCopAnalyzers/blob/master/documentation/SX1309.md
[SX1101]: https://github.com/DotNetAnalyzers/StyleCopAnalyzers/blob/master/documentation/SX1101.md
[SA1309]: https://github.com/DotNetAnalyzers/StyleCopAnalyzers/blob/master/documentation/SA1309.md
[SA1101]: https://github.com/DotNetAnalyzers/StyleCopAnalyzers/blob/master/documentation/SA1101.md
[stylecoprules]: https://github.com/DotNetAnalyzers/StyleCopAnalyzers/tree/master/documentation
[vs2019analyzers]: https://docs.microsoft.com/en-us/visualstudio/code-quality/configure-fxcop-analyzers?view=vs-2019#vs2019-163-and-later--fxcopanalyzers-package-version-33x-and-later
[ccmrepo]: https://github.com/riptano/ccm
[simulacronrepo]: https://github.com/datastax/simulacron
[simulacronreleases]: https://github.com/datastax/simulacron/releases
[dotnetcoresdk]: https://www.microsoft.com/net/download/core
