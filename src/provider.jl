@funsql begin

provider() = begin
    from(provider)
end

specialty_isa(args...) = in_vocabulary($Specialty, $args, specialty_concept_id)

provider_specialty_isa(args...) =
    in(provider_id, begin
        from(provider)
        filter(specialty_isa($args...))
        select(provider_id)
    end)

join_provider(ids...; carry=[]) = begin
    as(base)
    join(begin
        provider()
        $(length(ids) == 0 ? @funsql(define()) : @funsql filter(specialty_isa($ids...)))
    end, base.person_id == person_id)
    define($([@funsql($n => base.$n) for n in carry]...))
end

correlated_provider(ids...) = begin
	from(provider)
	filter(person_id == :person_id)
    $(length(ids) == 0 ? @funsql(define()) : @funsql filter(specialty_isa($ids...)))
	bind(:person_id => person_id )
end

with_provider_group(extension=nothing) =
    join(provider_group => begin
      from(provider)
      $(extension == nothing ? @funsql(define()) : extension)
      group(person_id)
    end, person_id == provider_group.person_id)

end
