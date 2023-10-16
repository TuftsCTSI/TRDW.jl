@funsql begin

specimen() = begin
    from(specimen)
    define(is_historical => specimen_id > 1500000000)
end

specimen_isa(ids...) = is_descendant_concept(specimen_concept_id, $ids...)
specimen_type_isa(ids...) = is_descendant_concept(specimen_type_concept_id, $ids...)

join_specimen(ids...; carry=[]) = begin
    as(base)
    join(begin
        specimen()
        $(length(ids) == 0 ? @funsql(define()) :
            @funsql filter(is_descendant_concept(specimen_concept_id, $ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_specimen(ids...) = begin
	from(specimen)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) :
        @funsql filter(is_descendant_concept(specimen_concept_id, $ids...)))
	bind(:person_id => person_id )
end

with_specimen_group(extension=nothing) =
    join(specimen_group => begin
      from(specimen)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == specimen_group.person_id)

end
