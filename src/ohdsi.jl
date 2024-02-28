module OHDSI

using CSV
using Pkg.Artifacts
using StringEncodings
using Tables

"""
    OHDSI.cohort_definition_to_md(str)

Take a string containing an OHDSI cohort definition and generate
its human-readable representation serialized in Markdown.
"""
function cohort_definition_to_md
end

"""
    OHDSI.concept_set_list_definition_to_md(str)

Take a string with a serialized list of concept sets and generate
its human-readable representation serialized in Markdown.
"""
function concept_set_list_definition_to_md
end

"""
    OHDSI.concept_set_definition_to_md(str)

Take a string containing an OHDSI concept set definition and generate
its human-readable representation serialized in Markdown.
"""
function concept_set_definition_to_md
end

"""
    OHDSI.cohort_definition_to_sql_template(str)

Take a string containing an OHDSI cohort definition and run OHDSI Circe
to generate the corresponding SQL template.
"""
function cohort_definition_to_sql_template
end

"""
    OHDSI.render_sql(template, params = (;))

Take a parameterized SQL template with query parameters and generate
an executable SQL query.
"""
function render_sql
end

"""
    OHDSI.translate_sql(sql, dialect = "spark", session_id = nothing, temp_emulation_schema = nothing)

Take a SQL query in the OHDSI source dialect and convert it to the target
SQL dialect.
"""
function translate_sql
end

"""
    OHDSI.split_sql(sql)

Split SQL code into individual queries.
"""
function split_sql
end

"""
    OHDSI.cohort_definition_to_sql(str; target_cohort_id, cdm_database_schema, ...)

Take a string containing an OHDSI cohort definition and run OHDSI Circe
to generate a vector of SQL queries.
"""
function cohort_definition_to_sql(
    str;
    target_cohort_id,
    cdm_database_schema,
    session_id = nothing,
    vocabulary_database_schema = cdm_database_schema,
    results_database_schema = cdm_database_schema,
    target_database_schema = cdm_database_schema,
    temp_emulation_schema = cdm_database_schema,
    target_cohort_table = :cohort,
    generate_stats = false,
)
    tmpl = cohort_definition_to_sql_template(str)
    sql = render_sql(
        tmpl,
        (;
         target_cohort_id,
         cdm_database_schema,
         vocabulary_database_schema,
         results_database_schema,
         target_database_schema,
         target_cohort_table,
         generateStats = Int(generate_stats)))
    tr = translate_sql(
        sql,
        temp_emulation_schema = temp_emulation_schema,
        session_id = session_id)
    tr′ = replace(tr, "\r\n" => '\n')
    String[stmt for stmt in split_sql(tr′) if !startswith(stmt, "TRUNCATE TABLE ")]
end

"""
    OHDSI.phenotype_library()

Return a vector of cohort definitions from the OHDSI Phenotype Library.
"""
function phenotype_library()
    csv = CSV.File(
        joinpath(artifact"PhenotypeLibrary", "PhenotypeLibrary-3.32.0/inst/Cohorts.csv"),
        select = [:cohortId, :cohortName, :logicDescription],
        types = Dict(:cohortId => Int, :cohortName => String, :logicDescription => String))
    ids = csv[:cohortId]
    names = csv[:cohortName]
    descriptions = csv[:logicDescription]
    definitions = String[decode(read(joinpath(artifact"PhenotypeLibrary", "PhenotypeLibrary-3.32.0/inst/cohorts/$id.json")), "latin1") for id in ids]
    Tables.rowtable((id = ids, name = names, description = descriptions, definition = definitions))
end

"""
    OHDSI.phenotype_library_v001()

Return a vector of cohort definitions from the OHDSI Phenotype Library v0.0.1.
"""
function phenotype_library_v001()
    all_ids = Int[]
    all_names = String[]
    all_descriptions = Union{String, Missing}[]
    all_definitions = String[]
    for dir in readdir(joinpath(artifact"PhenotypeLibrary-v0.0.1", "PhenotypeLibrary-0.0.1/inst"), join = true)
        isdir(dir) || continue
        csv = CSV.File(
            joinpath(dir, "cohortDescription.csv"),
            select = [:cohortId, :webApiCohortId, :cohortName, :logicDescription],
            types = Dict(:cohortId => Int, :webApiCohortId => Int, :cohortName => String, :logicDescription => String))
        basenames = csv[:cohortId]
        ids = csv[:webApiCohortId]
        names = csv[:cohortName]
        descriptions = csv[:logicDescription]
        definitions = String[decode(read(joinpath(dir, "$basename.json")), "latin1") for basename in basenames]
        append!(all_ids, ids)
        append!(all_names, names)
        append!(all_descriptions, descriptions)
        append!(all_definitions, definitions)
    end
    t = Tables.rowtable((id = all_ids, name = all_names, description = all_descriptions, definition = all_definitions))
    sort!(t)
end

end # module OHDSI
