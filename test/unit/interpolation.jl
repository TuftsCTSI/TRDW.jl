# Parameters used without `$` inside `@funsql`

const PACKAGE_ROOT = normpath(joinpath(@__DIR__, "..", ".."))
const SOURCE_FILES = included_sources(joinpath(PACKAGE_ROOT, "src", "TRDW.jl"))

# Bare names that are meant as column or table names, though a Julia name
# of the same name is in scope: (file, definition, name) => why.
const INTERPOLATION_ALLOWED = Pair{Tuple{String, Symbol, Symbol}, String}[
    ("src/template.jl", :NotebookFooter, :cdm_source) =>
        "reads the cdm_source table into a local variable of the same name",
]

allowed_name(file, def, name) =
    any(first(entry) == (file, def, name) for entry in INTERPOLATION_ALLOWED)

const ALL_DEFINITIONS = [
    d for path in SOURCE_FILES
    for d in funsql_definitions(read(path, String); file = relpath(path, PACKAGE_ROOT))]

@testset "@funsql parameters are interpolated" begin
    @test length(SOURCE_FILES) > 1
    @test any(d -> d.file == joinpath("src", "helpers.jl") && d.name == :antijoin, ALL_DEFINITIONS)

    @testset "$file" for file in unique(d.file for d in ALL_DEFINITIONS)
        @testset "$(d.name) (line $(d.line))" for d in filter(d -> d.file == file, ALL_DEFINITIONS)
            @test isempty([u for u in d.uses if !allowed_name(d.file, d.name, u.name)])
        end
    end

    @testset "$(entry[2]) in $(entry[1]) still uses $(entry[3]) bare" for (entry, reason) in INTERPOLATION_ALLOWED
        (file, def, name) = entry
        @test any(d -> d.file == file && d.name == def && any(u -> u.name == name, d.uses), ALL_DEFINITIONS)
    end
end
