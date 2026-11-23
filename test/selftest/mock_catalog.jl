function catalog_without(table, column)
    tables = Dict(
        name => FunSQL.SQLTable(name, columns = filter(!=(column), cols))
        for (name, cols) in cdm_columns() if name == table)
    for (name, t) in MOCK_CATALOG.tables
        haskey(tables, name) || (tables[name] = t)
    end
    FunSQL.SQLCatalog(tables = tables, dialect = MOCK_CATALOG.dialect, metadata = MOCK_CATALOG.metadata)
end

# For each table in EXTRA_COLUMNS, a query that reads all of its extra columns.
const EXTRA_COLUMN_QUERIES = Dict(
    :person => () -> @funsql(person()),
    :care_site => () -> @funsql(care_site()),
    :condition_occurrence => () -> @funsql(condition()),
    :drug_exposure => () -> @funsql(drug()),
    :measurement => () -> @funsql(measurement()),
    :note => () -> @funsql(note()),
    :visit_detail => () -> @funsql(visit_detail(Visit("Observation Room"))),
)

@testset "mock catalog" begin
    @testset "tables" begin
        @test length(MOCK_CATALOG) == 39
        @test MOCK_CATALOG.tables[:cohort].columns == MOCK_CATALOG.tables[:previous_cohort].columns
        @test :person_id in keys(MOCK_CATALOG.tables[:cohort].columns)
        @test !haskey(mock_catalog(previous = false).tables, :previous_cohort)
        @test Set(keys(EXTRA_COLUMN_QUERIES)) == Set(keys(EXTRA_COLUMNS))
    end

    @testset "$table.$column is needed" for (table, cols) in EXTRA_COLUMNS, column in cols
        q = EXTRA_COLUMN_QUERIES[table]()
        @test FunSQL.render(MOCK_CATALOG, q) isa FunSQL.SQLString
        @test_throws FunSQL.ReferenceError FunSQL.render(catalog_without(table, column), q)
    end
end
