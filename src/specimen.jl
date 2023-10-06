@funsql begin

specimen() = begin
    from(specimen)
end

specimen_isa(ids...) = is_descendant_concept(specimen_concept_id, $ids...)
specimen_type_isa(ids...) = is_descendant_concept(specimen_type_concept_id, $ids...)

correlated_specimen(ids...) = begin
	from(specimen)
	filter(person_id == :person_id)
	filter(is_descendant_concept(specimen_concept_id, $ids...))
	bind(:person_id => person_id )
end

with_specimen_group(extension=nothing) =
    join(specimen_group => begin
      from(specimen)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == specimen_group.person_id)

end
