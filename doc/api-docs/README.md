# How to build the C# Driver API Docs

1. Download and install [DocFX](https://dotnet.github.io/docfx/)
    - On linux you need `mono` because DocFX v2 runs on .NET Framework. DocFX will support .NET Core in v3.
2. `cd doc\api-docs`
3. run `docfx`
    - The static files of the website will be generated in `output`.
4. To preview the website you can **either**:
    - Open `output\index.html` on your browser
    - Or you can run `docfx serve output` which will spin up a web server on the `localhost`.
	
Note: if you don't have the `datastax-template`, docfx will still succeed but the generated website content will not have the DataStax theme and there won't be an `index.html` file at the root.
