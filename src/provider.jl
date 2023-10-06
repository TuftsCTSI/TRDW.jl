@funsql begin

provider() = begin
    from(provider)
end

specialty_isa(args...) = in_category(specialty_concept_id, $Specialty, $args)

provider_specialty_isa(args...) =
    in(provider_id, begin
        from(provider)
        filter(specialty_isa($args...))
        select(provider_id)
    end)

end
