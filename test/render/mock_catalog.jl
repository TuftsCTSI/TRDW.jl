const TABLE_QUERIES = [
    :person, :observation_period, :visit, :visit_detail, :condition,
    :procedure, :drug, :device, :measurement, :observation, :note,
    :provider, :care_site, :concept,
]

table_query(name) = getfield(TRDW, Symbol("funsql_", name))()

function catalog_without(table, column)
    tables = Dict(
        name => FunSQL.SQLTable(name, columns = filter(!=(column), cols))
        for (name, cols) in cdm_columns() if name == table)
    for (name, t) in MOCK_CATALOG.tables
        haskey(tables, name) || (tables[name] = t)
    end
    FunSQL.SQLCatalog(tables = tables, dialect = MOCK_CATALOG.dialect, metadata = MOCK_CATALOG.metadata)
end

const EXTRA_COLUMN_QUERIES = Dict(
    :person => :person, :care_site => :care_site,
    :condition_occurrence => :condition, :drug_exposure => :drug,
    :measurement => :measurement, :note => :note, :visit_detail => :visit_detail,
)

@testset "mock catalog" begin
    @testset "tables" begin
        @test length(MOCK_CATALOG) == 39
        @test MOCK_CATALOG.tables[:cohort].columns == MOCK_CATALOG.tables[:previous_cohort].columns
        @test :person_id in keys(MOCK_CATALOG.tables[:cohort].columns)
        @test !haskey(mock_catalog(previous = false).tables, :previous_cohort)
        @test Set(keys(EXTRA_COLUMN_QUERIES)) == Set(keys(EXTRA_COLUMNS))
    end

    @testset "$name() renders" for name in TABLE_QUERIES
        @test FunSQL.render(MOCK_CATALOG, table_query(name)) isa FunSQL.SQLString
    end

    @testset "$table.$column is needed" for (table, cols) in EXTRA_COLUMNS, column in cols
        cat = catalog_without(table, column)
        @test_throws FunSQL.ReferenceError FunSQL.render(cat, table_query(EXTRA_COLUMN_QUERIES[table]))
    end
end
