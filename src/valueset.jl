smoking_behavior_concepts() = [
        OMOP_Extension("OMOP5181846","Cigar smoker"),
        OMOP_Extension("OMOP5181838","Cigarette smoker"),
        OMOP_Extension("OMOP5181836","Electronic cigarette smoker"),
        OMOP_Extension("OMOP5181847","Hookah smoker"),
        OMOP_Extension("OMOP5181837","Passive smoker"),
        OMOP_Extension("OMOP5181845","Pipe smoker")]

never_smoker_concepts() = [OMOP_Extension("OMOP5181834", "Never used tobacco or its derivatives")]

@funsql smoking_behavior_concepts() = concept($(smoking_behavior_concepts())...)

@funsql matches_smoking_behavior() =
    concept_matches($(smoking_behavior_concepts()); match_on=value_as)

@funsql matches_never_smoker() =
    concept_matches($(never_smoker_concepts()); match_on=value_as)

function fetch_valuesets(ids)
    valueset_ids = join([HTTP.URIs.escapeuri(strip(x)) for x in ids], ",")
    apikey = ENV["UMLS_APIKEY"]
    valueset_url = "https://vsac.nlm.nih.gov/vsac/svs/RetrieveMultipleValueSets?id=$valueset_ids"
    encoded_apikey = HTTP.Base64.base64encode("apikey:$apikey")
    headers = ["Authorization" => "Basic $encoded_apikey"]
    HTTP.get(valueset_url, headers)
end


function parse_valuesets(s)
    xdoc = LightXML.parse_string(s)
    retval = []
    try
        xroot = root(xdoc)
        @assert name(xroot) == "RetrieveMultipleValueSetsResponse"
        for vs in get_elements_by_tagname(xroot, "DescribedValueSet")
            valueset = Dict()
            valueset["id"] = attribute(vs, "ID")
            valueset["name"] = attribute(vs, "displayName")
            valueset["purpose"] = content(get_elements_by_tagname(vs, "Purpose")[1])
            valueset["concepts"] = []
            for cl in get_elements_by_tagname(vs, "ConceptList")
                for c in get_elements_by_tagname(cl, "Concept")
                    concept = Dict()
                    concept["code"] = attribute(c, "code")
                    concept["codeSystemName"] = attribute(c, "codeSystemName")
                    concept["displayName"] = attribute(c, "displayName")
                    push!(valueset["concepts"], concept)
                end
            end
            push!(retval, valueset)
        end
    finally
        LightXML.free(xdoc)
    end
    return retval
end

resolve_lookup = Dict(
    "RXNORM" => RxNorm,
    "SNOMEDCT" => SNOMED
)

function resolve_valuesets!(valuesets; max=10)
    for vs in valuesets
        cnt = 0
        for cs in vs["concepts"]
            cnt += 1
            if cnt > max
                @error("attempt to resolve value set with more than $max concepts")
                break
            end
            vocab = get(resolve_lookup, cs["codeSystemName"], nothing)
            if isnothing(vocab)
                cs["concept"] = nothing
            else
                cs["concept"] = lookup_vsac_code(vocab, cs["code"])
            end
        end
    end
end

function valuesets(oids)
    VALUESET_PATH = joinpath(tempdir(), "valuesets")
    oids_to_fetch = [oid for oid in oids if !isfile(joinpath(VALUESET_PATH, oid))]
    res = fetch_valuesets([p for p in oids_to_fetch])
    for a in parse_valuesets(String(res.body))
        mkpath(VALUESET_PATH)
        open(joinpath(VALUESET_PATH, a["id"]),"w") do f
            JSON.print(f, a)
        end
    end
    ans = [JSON.parsefile(joinpath(VALUESET_PATH, oid)) for oid in oids]
    resolve_valuesets!(ans)
    csets = []
    htmls = []
    for oid in oids
        for a in ans
            if a["id"] == oid
                push!(csets, [c["concept"] for c in a["concepts"] if !isnothing(c["concept"])])
                push!(htmls, @htl("""
                    <dl>
                        <dt>Name</dt>
                        <dd>$(a["name"])</dd>
                        <dt>ID</dt>
                        <dd>$(a["id"])</dd>
                        <dt>Purpose</dt>
                        <dd>$(a["purpose"])</dd>
                        <dt>Concepts</dt>
                        <dd>
                        <table>
                            <tr>
                                <th>Resolved</th>
                                <th>Vocabulary</th>
                                <th>Code</th>
                                <th>Display Name</th>
                            </tr>
                            $([@htl("""
                                <tr>
                                    <td>
                                        $(isnothing(c["concept"]) ? "No" : "Yes")
                                    </td>
                                    <td>
                                        $(c["codeSystemName"])
                                    </td>
                                    <td>
                                        $(c["code"])
                                    </td>
                                    <td>
                                        $(c["displayName"])
                                    </td>
                                </tr>
                            """) for c in a["concepts"]])
                        </table>
                        </dd>
                    </dl>
                """))
            end
        end
    end
    htmls = @htl("""
        <br>
        $htmls
    """)
    return (htmls, csets)
end

function valueset(oid, name=nothing)

    res = fetch_valuesets([oid])

    ans = parse_valuesets(String(res.body))

    resolve_valuesets!(ans)

    ans = ans[1]

    if !isnothing(name) && ans["name"] != name
        error("Provided name does not match name of Valueset ($(ans["name"])).")
    end

    ret = Concept[]
    for a in ans["concepts"]
        if any(isnothing(a["concept"]))
            error("Valueset $oid has unresolved concept ($(a["codeSystemName"]), $(a["code"]))")
        end
        push!(ret, a["concept"])
    end

    return ret
end

macro valuesets(expr::Expr)
    @assert expr.head==:block
    oids = []
    len = convert(Int, (length(expr.args) / 2))
    for idx in range(1, len)
        item = expr.args[idx * 2]
        @assert item.head==:(=)
        (name, oid) = item.args
        item.args[1] = esc(name)
        push!(oids, item.args[2])
    end
    (htmls, csets) = valuesets(oids)
    push!(expr.args, HTML(htmls))
    for (idx, val) in enumerate(csets)
        expr.args[idx * 2].args[2] = val
    end
    return expr
end

vasc_vocabulary_lookup = Dict(
    "RXNORM" => "RxNorm",
    "SNOMEDCT" => "SNOMED",
)
vasc_vocab_lookup(name) = get(vasc_vocabulary_lookup, name, name)

function funsql_valueset(oid, name=nothing)
    res = fetch_valuesets([oid])
    ans = parse_valuesets(String(res.body))
    ans = ans[1]

    if !isnothing(name) && ans["name"] != name
        error("Provided name does not match name of Valueset ($(ans["name"])).")
    end

    uploaded_values = FunSQL.From(
     (vocabulary_id = [vasc_vocab_lookup(c["codeSystemName"]) for c in ans["concepts"]],
      concept_code = [c["code"] for c in ans["concepts"]],
      concept_name = [c["displayName"] for c in ans["concepts"]]))

    @funsql begin
        $uploaded_values
        left_join(c => concept(),
                  vocabulary_id == c.vocabulary_id &&
                  concept_code == c.concept_code)
        select(c.concept_id,
               vocabulary_id => coalesce(c.vocabulary_id, vocabulary_id),
               concept_code => coalesce(c.concept_code, concept_code),
               concept_name => coalesce(c.concept_name, concept_name))
    end
end
