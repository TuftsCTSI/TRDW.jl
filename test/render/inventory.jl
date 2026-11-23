const INVENTORY_CASES = [
    "frequency(args...)" =>
        () -> @funsql(person().frequency(gender_concept_id)),
    "tally(args...)" =>
        () -> @funsql(person().tally(gender_concept_id, race_concept_id)),
    "array_frequency(expr)" =>
        () -> @funsql(condition().group(person_id).define(codes => collect_list(concept_id)).array_frequency(codes)),
    "array_frequency(expr; name)" =>
        () -> @funsql(condition().group(person_id).define(codes => collect_list(concept_id)).array_frequency(codes, name = code)),
    "tally_array(expr)" =>
        () -> @funsql(condition().group(person_id).define(codes => collect_list(concept_id)).tally_array(codes)),
    "histogram(expr)" =>
        () -> @funsql(person().histogram(year_of_birth)),
    "histogram(expr, bins; name)" =>
        () -> @funsql(person().histogram(year_of_birth, 10, name = birth_year)),
    "percentiles(expr)" =>
        () -> @funsql(person().percentiles(year_of_birth)),
    "percentiles(expr, qs; name)" =>
        () -> @funsql(person().percentiles(year_of_birth, $([25, 50, 75]), name = birth_year)),
    "validate_primary_key(source, column)" =>
        () -> @funsql(validate_primary_key(person(), person_id)),
    "validate_primary_key(source, columns)" =>
        () -> @funsql(validate_primary_key(from(visit_detail), $([:visit_detail_id, :person_id]))),
    "validate_primary_key(table, column)" =>
        () -> @funsql(validate_primary_key(cohort, person_id)),
    "validate_primary_key(table, columns)" =>
        () -> @funsql(validate_primary_key(cohort, $([:person_id]))),
    "validate_primary_key(sources, columns)" =>
        () -> @funsql(validate_primary_key($([:cohort, :previous_cohort]), person_id)),
    "validate_foreign_key(source, column, target, column)" =>
        () -> @funsql(validate_foreign_key(condition(), person_id, person(), person_id)),
    "validate_foreign_key(source, columns, target, columns)" =>
        () -> @funsql(validate_foreign_key(
            from(visit_detail), $([:visit_occurrence_id, :person_id]),
            from(visit_occurrence), $([:visit_occurrence_id, :person_id]))),
    "validate_foreign_key(source, columns, target_table, columns)" =>
        () -> @funsql(validate_foreign_key(condition(), $([:person_id]), person, $([:person_id]))),
    "validate_foreign_key(source_table, columns, target, columns)" =>
        () -> @funsql(validate_foreign_key(cohort, $([:person_id]), person(), $([:person_id]))),
    "validate_foreign_key(table, column, table, column)" =>
        () -> @funsql(validate_foreign_key(cohort, person_id, person, person_id)),
    "validate_foreign_key(sources, columns, target, columns)" =>
        () -> @funsql(validate_foreign_key($([:cohort, :previous_cohort]), person_id, person, person_id)),
    "validate_foreign_key(source, columns, target)" =>
        () -> @funsql(validate_foreign_key(cohort, $([:person_id]), person())),
    "row_delta(table, keys)" =>
        () -> @funsql(row_delta(cohort, $([:person_id]))),
    "row_delta(table, keys, previous_table)" =>
        () -> @funsql(row_delta(person, $([:person_id]), cohort)),
    "column_delta(table, keys)" =>
        () -> @funsql(column_delta(cohort, $([:person_id]))),
    "column_delta(table, keys, previous_table)" =>
        () -> @funsql(column_delta(cohort, $([:person_id]), previous_cohort)),
]

# Cases that render against a catalog with no `previous_cohort`, so `if_set`
# takes its fallback.
const INVENTORY_NO_PREVIOUS_CASES = [
    "row_delta(table, keys), with no previous table" =>
        () -> @funsql(row_delta(cohort, $([:person_id]))),
    "column_delta(table, keys), with no previous table" =>
        () -> @funsql(column_delta(cohort, $([:person_id]))),
]

const NO_PREVIOUS_CATALOG = mock_catalog(previous = false)

@testset "inventory.jl" begin
    @testset "$name renders" for (name, build) in INVENTORY_CASES
        @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
    end
    @testset "$name renders" for (name, build) in INVENTORY_NO_PREVIOUS_CASES
        @test with_test_env(() -> build_and_render(build, catalog = NO_PREVIOUS_CATALOG, context = name)) !== nothing
    end
end
