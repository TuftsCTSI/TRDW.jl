# PlutoFun
This is a Pluto environment with FunSQL for use in Research Requests.

To add this to a repository, you'll want to:

```
$ julia --project=.
julia> ]
pkg> add https://github.com/MechanicalRabbit/FunSQL.jl#funsql-macro
pkg> add git@github.com:TuftsCTSI/PlutoFun.git
$ echo -e 'pluto:\n\tjulia --project=. -e "using Pkg; Pkg.instantiate(); using Pluto; Pluto.run()"' > Makefile
$ git add Project.toml Manifest.toml Makefile
```

Then, after you ensure you have the proper DATABRICKS_ envrionment variables, you could change to the given directory and type "make".
