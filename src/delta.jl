@funsql begin

"""
    @funsql delta_person()

# Examples

```julia
@funsql delta_person()
```
"""
delta_person() = begin
	from(latest_person)
	join(previous => from(previous_person), person_id == previous.person_id, left=true, right=true)
	select(latest_person_id => person_id, latest_pat_id => pat_id, previous_person_id => previous.person_id, previous_pat_id => previous.pat_id)
end

"""
    @funsql person_snapshot_delta()

# Examples

```julia
@funsql person_snapshot_delta()
```
"""
person_snapshot_delta() = append(
	delta_person().filter(isnull(previous_person_id)).select(person_id => latest_person_id, pat_id => latest_pat_id, result => "added"),
	delta_person().filter(isnull(latest_person_id)).select(person_id => previous_person_id, pat_id => previous_pat_id, result => "disappeared"),
	person_records_changed().select(person_id, pat_id, result => "changed"),
)

"""
    @funsql person_records_changed()

# Examples

```julia
@funsql person_records_changed()
```
"""
person_records_changed() = begin
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
end

"""
    @funsql snapshot_delta()

Create a summary table for all OMOP tables in any two TRDW-Prime snapshots.

# Examples 

```julia
@funsql snapshot_delta()
```
"""
snapshot_delta() = append(
	person_snapshot_delta().group(result).define(count(), table => "person"),
	# visit_occurrence_snapshot_delta().group(result).define(count(), table => "visit_occurrence"),
	# provider_snapshot_delta().group(result).define(count(), table => "provider"),
	# care_site_snapshot_delta().group(result).define(count(), table => "care_site"),
	# condition_occurrence_snapshot_delta().group(result).define(count(), table => "condition_occurrence"),
	# observation_snapshot_delta().group(result).define(count(), table => "observation"),
	# measurement_snapshot_delta().group(result).define(count(), table => "measurement"),
	# observation_period_snapshot_delta().group(result).define(count(), table => "observation_period")
)






end