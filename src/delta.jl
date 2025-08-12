# """
#     @funsql define_is_equal()

#     A helper function used to build the "records changed" tables.

#     Given a list of variables, define 

# # Examples

# ```julia
# schema="trdw_prime_latest"
# reference_schema="trdw_prime_previous"
# comparison_variables=[:pat_id, :mother_person_id, :father_person_id,
#     :gender_concept_id, :birth_datetime, :death_datetime, 
#     :race_concept_id, :ethnicity_concept_id, :provider_id]
# @funsql begin
#     from(latest_person)
# 	join(previous => from(previous_person), person_id == previous.person_id)
# 	select(
#         person_id,
# 		pat_id
#     )
#     define_is_equal(comparison_columns)
#     delta_ctes("person", schema, reference_schema, ([:person_id; comparison_columns]))
# end
# ```
# """
# function funsql_define_is_equal(variable)
#     name = "$(variable)_is_equal"
#     @funsql define($name => $variable == previous.$(variable) || (isnull($variable) && isnull(previous.$(variable))) ? true : false)
# end

function funsql_delta_ctes(table, schema, reference_schema, columns)
    latest = "latest_$table"
    previous = "previous_$table"
    @funsql begin
        with(
            $latest => from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(schema)], name = Symbol(table), columns = columns))),
            $previous => from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(reference_schema)], name = Symbol(table), columns = columns))))
    end
end

@funsql begin

delta_person(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(`latest_person`)
	join(previous => from(`previous_person`), person_id == previous.person_id, left=true, right=true)
	select(latest_person_id => person_id, latest_pat_id => pat_id, previous_person_id => previous.person_id, previous_pat_id => previous.pat_id)
    delta_ctes("person", $schema, $reference_schema, $([:person_id, :pat_id]))
end


# delta_person(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
    # 	from(`latest_person`)
    # 	join(previous => from(`previous_person`), person_id == previous.person_id, left=true, right=true)
    # 	select(latest_person_id => person_id, latest_pat_id => pat_id, previous_person_id => previous.person_id, previous_pat_id => previous.pat_id)
    #     with(`latest_person` =>
    #             from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(schema)], name = :person,
    #                                    columns = [:person_id, :pat_id]))),
    #                                    `previous_person` =>
    #             from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(reference_schema)], name = :person,
    #                                    columns = [:person_id, :pat_id])))
    #     )
    # end
    
    
#WIP - why does this timeout?
delta_visit_occurrence(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
    from(`latest_visit_occurrence`)
    join(previous => from(`previous_visit_occurrence`), visit_occurrence_id == previous.visit_occurrence_id, left=true, right=true)
    select(latest_visit_occurrence_id => visit_occurrence_id, latest_pat_enc_csn_id => pat_enc_csn_id, previous_visit_occurrence_id => previous.visit_occurrence_id, previous_pat_enc_csn_id => previous.pat_enc_csn_id)
    delta_ctes("visit_occurrence", $schema, $reference_schema, $([:visit_occurrence_id, :pat_enc_csn_id]))
end

# WIP - needs define_is_equal
person_records_changed2(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_person)
	join(previous => from(previous_person), person_id == previous.person_id)
	select(
        person_id,
		pat_id
    )
    define_is_equal(pat_id)
    with(`latest_person` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(schema)], name = :person,
                                   columns = [:person_id, :pat_id,
    :mother_person_id,
    :father_person_id,
    :gender_concept_id,
    :birth_datetime,
    :death_datetime,
    :race_concept_id,
    :ethnicity_concept_id,
    :provider_id
]))),
                                   `previous_person` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(reference_schema)], name = :person,
                                   columns = [:person_id, :pat_id, 
    :mother_person_id,
    :father_person_id,
    :gender_concept_id,
    :birth_datetime,
    :death_datetime,
    :race_concept_id,
    :ethnicity_concept_id,
    :provider_id
])))
    )
end

"""
@funsql person_records_changed()

# Examples

```julia
@funsql person_records_changed()
```
"""
person_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_person)
	join(previous => from(previous_person), person_id == previous.person_id)
	select(
        person_id,
		pat_id,
		pat_id_is_equal => pat_id == previous.pat_id || (isnull(pat_id) && isnull(previous.pat_id)) ? true : false,
		mother_person_id_is_equal => mother_person_id == previous.mother_person_id || (isnull(mother_person_id) && isnull(previous.mother_person_id)) ? true : false,
		father_person_id_is_equal => father_person_id == previous.father_person_id || (isnull(father_person_id) && isnull(previous.father_person_id)) ? true : false,
		gender_concept_id_is_equal => gender_concept_id == previous.gender_concept_id || (isnull(gender_concept_id) && isnull(previous.gender_concept_id)) ? true : false,
		birth_datetime_is_equal => birth_datetime == previous.birth_datetime || (isnull(birth_datetime) && isnull(previous.birth_datetime)) ? true : false,
		death_datetime_is_equal => death_datetime == previous.death_datetime || (isnull(death_datetime) && isnull(previous.death_datetime)) ? true : false,
		race_concept_id_is_equal => race_concept_id == previous.race_concept_id || (isnull(race_concept_id) && isnull(previous.race_concept_id)) ? true : false,
		ethnicity_concept_id_is_equal => ethnicity_concept_id == previous.ethnicity_concept_id || (isnull(ethnicity_concept_id) && isnull(previous.ethnicity_concept_id)) ? true : false,
		provider_id_is_equal => provider_id == previous.provider_id || (isnull(provider_id) && isnull(previous.provider_id)) ? true : false
	)
	define(
		record_unchanged => (
            pat_id_is_equal &&
			mother_person_id_is_equal && 
			father_person_id_is_equal &&
			gender_concept_id_is_equal &&
			birth_datetime_is_equal &&
			death_datetime_is_equal &&
			race_concept_id_is_equal &&
			ethnicity_concept_id_is_equal &&
			provider_id_is_equal
            ) ? true : false
    )
    filter(!record_unchanged)
    with(`latest_person` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(schema)], name = :person,
                                   columns = [:person_id, :pat_id,
    :mother_person_id,
    :father_person_id,
    :gender_concept_id,
    :birth_datetime,
    :death_datetime,
    :race_concept_id,
    :ethnicity_concept_id,
    :provider_id
]))),
                                   `previous_person` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(reference_schema)], name = :person,
                                   columns = [:person_id, :pat_id, 
    :mother_person_id,
    :father_person_id,
    :gender_concept_id,
    :birth_datetime,
    :death_datetime,
    :race_concept_id,
    :ethnicity_concept_id,
    :provider_id
])))
    )
        end

        """
            @funsql person_snapshot_delta()
        
        # Examples
        
        ```julia
        @funsql person_snapshot_delta()
        ```
        """
    person_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
        delta_person($schema, $reference_schema).filter(isnull(previous_person_id)).select(person_id => latest_person_id, pat_id => latest_pat_id, result => "added"),
        delta_person($schema, $reference_schema).filter(isnull(latest_person_id)).select(person_id => previous_person_id, pat_id => previous_pat_id, result => "disappeared"),
        person_records_changed($schema, $reference_schema).select(person_id, pat_id, result => "changed"),
    )
        
        """
        @funsql snapshot_delta()
        
        Create a summary table for all OMOP tables in any two TRDW-Prime snapshots.
            
            # Examples 

```julia
@funsql snapshot_delta()
```
"""
snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	person_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "person"),
	# visit_occurrence_snapshot_delta().group(result).define(count(), table => "visit_occurrence"),
	# provider_snapshot_delta().group(result).define(count(), table => "provider"),
	# care_site_snapshot_delta().group(result).define(count(), table => "care_site"),
	# condition_occurrence_snapshot_delta().group(result).define(count(), table => "condition_occurrence"),
	# observation_snapshot_delta().group(result).define(count(), table => "observation"),
	# measurement_snapshot_delta().group(result).define(count(), table => "measurement"),
	# observation_period_snapshot_delta().group(result).define(count(), table => "observation_period")
)



delta_x() = person().limit(1)


end