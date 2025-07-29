# How to build the C# Driver API Docs

1. Download and install [DocFX](https://dotnet.github.io/docfx/)
    - On linux you need `mono` because DocFX v2 runs on .NET Framework. DocFX will support .NET Core in v3.
2. `cd docs/source/api-docs`
3. run `docfx`
    - The static files of the website will be generated in a new subdirectory called `api-docs`.
    - You might see some warnings about not being able to resolve base documentation depending on the version of DocFX that you use. Unfortunately this is expected and it's because [DocFX doesn't have full support for `inheritdoc`](https://github.com/dotnet/docfx/issues/3699#issuecomment-444039674).
4. To preview the website you can **either**:
    - Open `/api-docs/index.html` on your browser
    - Or you can run `docfx serve api-docs` which will spin up a web server on the `localhost`.

Note: if you don't have the custom template mentioned on docfx.json, docfx will still succeed but the generated website content will not have the DataStax theme and there won't be an `index.html` file at the root.
