export DrawIO

const DRAWIO_NEW_SVG =
    """
    <svg xmlns="http://www.w3.org/2000/svg" width="120" height="60">
      <rect x="2" y="2" width="116" height="56" rx="9" ry="9" fill="none" stroke="#666666" stroke-width="2" stroke-dasharray="6,6"/>
      <text x="60" y="30" text-anchor="middle" dominant-baseline="central" font-family="&quot;Alegreya Sans&quot;" font-size="16px" font-style="italic" fill="#333333">Click to edit</text>
    </svg>
    """

const DRAWIO_ORIGIN = "https://embed.diagrams.net"

const DRAWIO_URL = "https://embed.diagrams.net/?proto=json&libraries=1&configure=1&noSaveBtn=1"

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
        return print(io, "<figure>$svg</figure>")
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

          const createFrameNode = () => {
            frameNode = document.createElement("iframe")
            frameNode.style.position = "fixed"
            frameNode.style.top = "0"
            frameNode.style.left = "0"
            frameNode.style.width = "100%"
            frameNode.style.height = "100%"
            frameNode.style.border = "0"
            frameNode.style.zIndex = "1000"
            frameNode.style.opacity = "0"
            frameNode.style.transition = "opacity 0.2s"
            frameNode.style.visibility = "hidden"
            frameNode.src = $DRAWIO_URL
            figureNode.append(frameNode)
            figureNode.style.cursor = "wait"
            figureNode.removeAttribute("title")
          }

          const revealFrameNode = () => {
            frameNode.style.visibility = "visible"
            frameNode.style.opacity = "1"
          }

          const removeFrameNode = () => {
            frameNode.remove()
            frameNode = null
            figureNode.style.cursor = "pointer"
            figureNode.title = "Click to edit"
          }

          const onMessage = (e) => {
            if (!frameNode) return
            if (e.origin !== $DRAWIO_ORIGIN) return
            if (e.source !== frameNode.contentWindow) return
            const msg = JSON.parse(e.data)
            //console.log(msg)
            let action = null
            if (msg.event == "configure") {
              action = { action: "configure", config: $DRAWIO_CONFIG }
            }
            else if (msg.event == "init") {
              const content = svgNode.getAttribute("content")
              if (content) {
                action = { action: "load", xml: content }
              }
              else {
                revealFrameNode()
                action = { action: "template" }
              }
            }
            else if (msg.event == "load") {
              revealFrameNode()
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
              removeFrameNode()
            }
            else if (msg.event == "exit") {
              removeFrameNode()
            }
            if (action) {
              //console.log(action)
              e.source.postMessage(JSON.stringify(action), $DRAWIO_ORIGIN)
            }
          }
          window.addEventListener("message", onMessage)

          const onClick = () => {
            if (frameNode) return
            createFrameNode()
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
