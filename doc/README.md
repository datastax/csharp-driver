# How to generate api docs

## Requirements

- Install [Sandcastle Help File Builder](https://github.com/EWSoftware/SHFB/releases).

## Procedure

1. Open the `DataStaxStyle.sln` solution and build the project in order to build the DataStax Presentation Style.
2. Open `Documentation.shfbproj` with the Sandcastle standalone GUI and build the project. The api docs will be generated into the `./api-docs` directory.

Note that the Sandcastle build will lock the presentation style binary file so if you need to close it if you want to build the presentation style project again.
