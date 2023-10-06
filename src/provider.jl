@funsql begin

is_specialty(args...) = in_category(specialty, $Specialty, $args)

is_provider_specialty(args...) =
    in(provider_id, begin
        from(provider)
        filter(is_specialty($args...))
        select(provider_id)
    end)

end
