@funsql begin

measurement() = begin
    from(measurement)
end

measurement_isa(ids...) = is_descendant_concept(measurement_concept_id, $ids...)
measurement_type_isa(ids...) = is_descendant_concept(measurement_type_concept_id, $ids...)

correlated_measurement(ids...) = begin
	from(measurement)
	filter(person_id == :person_id)
	filter(is_descendant_concept(measurement_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_measurement_group(extension=nothing) =
    join(measurement_group => begin
      from(measurement)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == measurement_group.person_id)

end
