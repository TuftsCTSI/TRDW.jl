format_as_enum(q::FunSQL.SQLNode) = @funsql begin
    from(concept)
    filter(standard_concept=="S")
    filter(is_null(invalid_reason))
    $q
    order(concept_id)
    select(enum => concat(lower(replace(concept_name, " ", "_")), " = ", concept_id))
end
print_as_enum(db::FunSQL.SQLConnection, q::FunSQL.SQLNode) =
    print("        " * join(TRDW.run(db, format_as_enum(q)).enum, "\n        "))

module Race
    # filter(domain_id == "Race").filter(concept_id<10000)
    @enum T begin
        asian = 8515
        black_or_african_american = 8516
        white = 8527
        native_hawaiian_or_other_pacific_islander = 8557
        american_indian_or_alaska_native = 8657
    end
end

module Ethnicity
    # filter(domain_id == "Ethnicity")
    @enum T begin
        hispanic_or_latino = 38003563
        not_hispanic_or_latino = 38003564
    end
end

