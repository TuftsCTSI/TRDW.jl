export DrawIO

const DRAWIO_NEW_SVG =
    """
    <svg xmlns="http://www.w3.org/2000/svg" width="120" height="90">
      <rect x="2" y="2" width="116" height="86" rx="15" ry="15" fill="none" stroke ="silver" stroke-width="2" stroke-dasharray="8,4"/>
    </svg>
    """

const DRAWIO_ORIGIN = "https://embed.diagrams.net"

const DRAWIO_URL = "https://embed.diagrams.net/?proto=json&spin=1&libraries=1&configure=1&noSaveBtn=1"

const DRAWIO_CONFIG =
    (
        defaultFonts = ["Alegreya Sans", "Vollkorn", "JuliaMono"],
        defaultVertexStyle = (fontFamily = "Alegreya Sans", fontSize = 16),
        defaultEdgeStyle = (fontFamily = "Alegreya Sans", fontSize = 16),
        simpleLabels = true,
        pageFormat = (width = 600, height = 450),
        css =
            """
            @import url("https://cdn.jsdelivr.net/gh/fonsp/Pluto.jl@0.20.4/frontend/fonts/alegreya.css");
            @import url("https://cdn.jsdelivr.net/gh/fonsp/Pluto.jl@0.20.4/frontend/fonts/vollkorn.css");
            @import url("https://cdn.jsdelivr.net/gh/fonsp/Pluto.jl@0.20.4/frontend/fonts/juliamono.css");
            """,
    )

struct DrawIO
    filename::String
    editable::Bool
    data::Any

    DrawIO(filename; editable = get(ENV, "CI", nothing) != "true", data = nothing) =
        new(filename, editable, Tables.istable(data) ? only(Tables.rowtable(data)) : data)
end

function _drawio_substitute(drawio::DrawIO, s)
    name = Symbol(s[3:end-1])
    value =
        if drawio.data isa AbstractDict
            name in keys(drawio.data) || return s
            drawio.data[name]
        else
            hasfield(typeof(drawio.data), name) || return s
            getfield(drawio.data, name)
        end
    value === nothing ? "" : HTTP.Strings.escapehtml(string(value))
end

function Base.show(io::IO, mime::MIME"text/html", drawio::DrawIO)
    svg = DRAWIO_NEW_SVG
    if isfile(drawio.filename)
        svg = read(drawio.filename, String)
        if drawio.data !== nothing
            svg = replace(svg, r"\$\(\w+\)" => Base.Fix1(_drawio_substitute, drawio))
        end
    end
    if !drawio.editable
        return print(io, "<div><figure>$svg</figure></svg>")
    end
    id = "drawio-$(objectid(drawio))"
    show(
        io,
        mime,
        @htl """
        <script id="$id">
          const svgNode = (new DOMParser()).parseFromString($svg, "image/svg+xml").firstElementChild
          console.assert(svgNode?.tagName == "svg")
          const save = $(AbstractPlutoDingetjes.Display.with_js_link((svg) -> write(drawio.filename, svg)))
          const cellNode = currentScript.closest("pluto-cell")
          let figureNode = this
          if (!figureNode) {
            figureNode = document.createElement("figure")
            figureNode.style.cursor = "pointer"
            figureNode.title = "Click to edit"
          }
          else {
            Array.from(figureNode.children).filter((c) => c.tagName == "svg").forEach((c) => c.remove())
          }
          let frameNode = Array.from(figureNode.children).find((c) => c.tagName == "IFRAME") ?? null
          figureNode.prepend(svgNode)

          const onMessage = (e) => {
            if (!frameNode) return
            if (e.origin !== $DRAWIO_ORIGIN) return
            if (e.source !== frameNode.contentWindow) return
            const msg = JSON.parse(e.data)
            console.log(msg)
            let action = null
            if (msg.event == "configure") {
              action = { action: "configure", config: $DRAWIO_CONFIG }
            }
            else if (msg.event == "init") {
              frameNode.style.opacity = "1"
              const content = svgNode.getAttribute("content")
              if (content) {
                action = { action: "load", xml: content }
              }
              else {
                action = { action: "template" }
              }
            }
            else if (msg.event == "save") {
              action = {
                action: "export",
                format: "xmlsvg",
                xml: msg.xml,
                spinKey: "export",
                border: 1,
              }
            }
            else if (msg.event == "export") {
              const prefix = "data:image/svg+xml;base64,"
              console.assert(msg.data.startsWith(prefix))
              const svg = atob(msg.data.slice(prefix.length))
              save(svg)
              cellNode._internal_pluto_actions.set_and_run_multiple([cellNode.getAttribute("id")])
              frameNode.remove()
              frameNode = null
            }
            else if (msg.event == "exit") {
              frameNode.remove()
              frameNode = null
            }
            if (action) {
              console.log(action)
              e.source.postMessage(JSON.stringify(action), $DRAWIO_ORIGIN)
            }
          }
          window.addEventListener("message", onMessage)

          const onClick = () => {
            if (frameNode) return
            frameNode = document.createElement("iframe")
            frameNode.style.position = "fixed"
            frameNode.style.top = "0"
            frameNode.style.left = "0"
            frameNode.style.width = "100%"
            frameNode.style.height = "100%"
            frameNode.style.border = "0"
            frameNode.style.zIndex = "1000"
            frameNode.style.opacity = "0.9"
            frameNode.style.transition = "opacity 0.2s"
            frameNode.src = $DRAWIO_URL
            figureNode.append(frameNode)
            console.log(frameNode.tagName)
          }
          figureNode.addEventListener("click", onClick)

          invalidation.then(() => {
            window.removeEventListener("message", onMessage)
            figureNode.removeEventListener("click", onClick)
          })

          return figureNode
        </script>
        """
    )
end
