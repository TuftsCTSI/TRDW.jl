using CSV
using DataFrames
using FunSQL
using TRDW

const CDM_FIELDS_PATH =
    joinpath(@__DIR__, "..", "fixtures", "cdm", "OMOP_CDMv5.4_Field_Level.csv")

# Columns TRDW reads that aren't in the CDM 5.4 file.
const EXTRA_COLUMNS = Dict{Symbol, Vector{Symbol}}(
    # person() (src/omop.jl), defines death_date from it
    :person => [:death_datetime],
    # care_site() (src/omop.jl)
    :care_site => [:care_site_source_concept_id],
    # condition() (src/omop.jl)
    :condition_occurrence => [:condition_status_source_concept_id],
    # drug() (src/omop.jl)
    :drug_exposure => [:drug_mode_concept_id, :fill_unit_source_value, :rate_unit_source_value],
    # measurement() (src/omop.jl)
    :measurement => [:measurement_mode_concept_id],
    # note() (src/omop.jl)
    :note => [:note_source_concept_id],
    # visit_detail() (src/omop.jl); visit_detail(concept_set) also filters
    # on the four *_source_concept_id columns after them
    :visit_detail => [
        :admitted_from_source_concept_id, :discharged_to_source_concept_id,
        :base_class_source_concept_id, :class_source_concept_id,
        :service_source_concept_id, :lvl_of_care_source_concept_id],
)

# Table name => column names, in the CDM file's order, plus EXTRA_COLUMNS.
function cdm_columns(path = CDM_FIELDS_PATH)
    cdm = CSV.read(path, DataFrame, select = [:cdmTableName, :cdmFieldName])
    columns = Dict{Symbol, Vector{Symbol}}()
    for g in groupby(cdm, :cdmTableName, sort = false)
        name = Symbol(lowercase(first(g.cdmTableName)))
        cols = Symbol.(lowercase.(g.cdmFieldName))
        for col in get(EXTRA_COLUMNS, name, Symbol[])
            col in cols || push!(cols, col)
        end
        columns[name] = cols
    end
    for name in keys(EXTRA_COLUMNS)
        haskey(columns, name) || error("EXTRA_COLUMNS names a table not in the CDM file: $name")
    end
    columns
end

const CDM_EXCLUDED_TABLES = [:cohort, :cohort_definition]
const COHORT_COLUMNS = [:person_id, :cohort_entry_date]

function mock_catalog(; previous = true)
    tables = Dict{Symbol, FunSQL.SQLTable}()
    for (name, cols) in cdm_columns()
        name in CDM_EXCLUDED_TABLES && continue
        tables[name] = FunSQL.SQLTable(name, columns = cols)
    end
    tables[:cohort] = FunSQL.SQLTable(:cohort, columns = COHORT_COLUMNS)
    if previous
        tables[:previous_cohort] = FunSQL.SQLTable(:previous_cohort, columns = COHORT_COLUMNS)
    end
    FunSQL.SQLCatalog(
        tables = tables,
        dialect = FunSQL.SQLDialect(:spark),
        metadata = (; trdw = TRDW.CatalogMetadata("ctsi", nothing)))
end

const MOCK_CATALOG = mock_catalog()
