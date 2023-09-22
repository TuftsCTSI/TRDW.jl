# PlutoFun
This is a Pluto environment with FunSQL for use in Research Requests.

To add this to a repository, you'll want to:

```
$ julia --project=.
julia> ]
pkg> add https://github.com/MechanicalRabbit/FunSQL.jl#funsql-macro
pkg> add https://github.com/JuliaDatabases/ODBC.jl
pkg> add https://github.com/MechanicalRabbit/Pluto.jl#funsql
pkg> add git@github.com:TuftsCTSI/PlutoFun.git
pkg> add PlutoUI PlutoPlotly
$ echo -e 'pluto:\n\tjulia --project=. -e "using Pkg; Pkg.instantiate(); using Pluto; Pluto.run()"' > Makefile
$ git add Project.toml Manifest.toml Makefile
```

Alternatively, you could copy these three files, `Project.toml`, `Manifest.toml` and `Makefile` from another working project.

Then, after you ensure you have the proper DATABRICKS_ envrionment variables, you could change to the given directory and type "make".
