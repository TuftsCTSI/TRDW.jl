const HELPERS_CASES = [
    "antijoin(q, lhs), rhs defaults to lhs" =>
        () -> @funsql(person().antijoin(condition(), person_id)),
    "antijoin(q, lhs, rhs), rhs differs from lhs" =>
        () -> @funsql(from(concept).antijoin(from(concept_relationship), concept_id, concept_id_1)),
    "restrict_by(q)" =>
        () -> @funsql(condition().restrict_by(from(cohort))),
    "restrict_by(column_name, q)" =>
        () -> @funsql(visit().restrict_by(visit_occurrence_id, condition())),
    "like_acronym(s, pat), without a space" =>
        () -> @funsql(concept().filter(like_acronym(concept_name, "HTN"))),
    "like_acronym(s, pat), with a space" =>
        () -> @funsql(concept().filter(like_acronym(concept_name, "high blood"))),
    "like_acronym(s, pats...)" =>
        () -> @funsql(concept().filter(like_acronym(concept_name, "HTN", "high blood"))),
    "icontains(s, pat)" =>
        () -> @funsql(concept().filter(icontains(concept_name, "hypertension"))),
    "icontains(s, pats::Vector)" =>
        () -> @funsql(concept().filter(icontains(concept_name, $(["essential", "hypertension"])))),
    "icontains(s, pats::NTuple)" =>
        () -> @funsql(concept().filter(icontains(concept_name, $(("essential", "hypertension"))))),
    "icontains(s, pat, pats...)" =>
        () -> @funsql(concept().filter(icontains(concept_name, "hypertension", "hypertensive"))),
    "is_integer(s)" =>
        () -> @funsql(concept().filter(is_integer(concept_code))),
    "roundup(n)" =>
        () -> @funsql(person().group().define(n => roundup(count()))),
    "collect_to_string(v)" =>
        () -> @funsql(condition().group(person_id).define(codes => collect_to_string(concept_id))),
    "collect_list_ordered(value)" =>
        () -> @funsql(condition().group(person_id).define(codes => collect_list_ordered(concept_id, by = datetime))),
    "collect_set_ordered(value)" =>
        () -> @funsql(condition().group(person_id).define(codes => collect_set_ordered(concept_id, by = datetime))),
    "deduplicate(keys...)" =>
        () -> @funsql(condition().deduplicate(person_id)),
    "deduplicate(keys...; order_by)" =>
        () -> @funsql(condition().deduplicate(person_id, order_by = [datetime])),
]

@testset "helpers.jl" begin
    @testset "$name renders" for (name, build) in HELPERS_CASES
        @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
    end
end
