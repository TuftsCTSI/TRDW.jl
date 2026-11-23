const COUNTING_CASES = [
    "demographics(; roundup = false)" =>
        () -> @funsql(from(cohort).demographics(roundup = false)),
    "define_n_person(; roundup = false)" =>
        () -> @funsql(from(cohort).group(person_id).define_n_person(roundup = false)),
    "stratify_by(groups...; roundup = false)" =>
        () -> @funsql(condition().stratify_by(concept_id, roundup = false)),
    "stratify_by_age(; roundup = false)" =>
        () -> @funsql(from(cohort).stratify_by_age(roundup = false)),
    "stratify_by_age(pair; roundup)" =>
        () -> @funsql(stratify_by_age(cohort => from(cohort), roundup = true)),
    "stratify_by_race(; roundup = false)" =>
        () -> @funsql(from(cohort).stratify_by_race(roundup = false)),
    "stratify_by_race(pair; roundup)" =>
        () -> @funsql(stratify_by_race(cohort => from(cohort), roundup = true)),
    "stratify_by_sex(; roundup = false)" =>
        () -> @funsql(from(cohort).stratify_by_sex(roundup = false)),
    "stratify_by_sex(pair; roundup)" =>
        () -> @funsql(stratify_by_sex(cohort => from(cohort), roundup = true)),
    "stratify_by_ethnicity(; roundup = false)" =>
        () -> @funsql(from(cohort).stratify_by_ethnicity(roundup = false)),
    "stratify_by_ethnicity(pair; roundup)" =>
        () -> @funsql(stratify_by_ethnicity(cohort => from(cohort), roundup = true)),
    "stratify_by_translator(; roundup = false)" =>
        () -> @funsql(from(cohort).stratify_by_translator(roundup = false)),
    "stratify_by_preferred_language(; roundup = false)" =>
        () -> @funsql(from(cohort).stratify_by_preferred_language(roundup = false)),
]

@testset "counting.jl" begin
    @testset "$name renders" for (name, build) in COUNTING_CASES
        @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
    end
end
