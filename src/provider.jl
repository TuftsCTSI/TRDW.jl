@funsql begin

is_specialty(args...) = 
    in(specialty_concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id,
                  $([Integer(getfield(Specialty, x)) for x in args])...))
        select(descendant_concept_id)
    end)

end
