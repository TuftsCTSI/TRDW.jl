function NotebookFooter()
    config = config_file()
    IRB_CODE = config[:irb_code]
    PROJECT_ID = config[:project_id]
    PROJECT_CODE = config[:project_code]
    PROJECT_SLUG = config[:project_slug]
  @htl("""
  <div>
    <table style="width: 100%">
    <tr><td style="width: 72; vertical-align: top">
      <small>
        Produced by <a href="https://www.tuftsctsi.org/">
        Tufts Clinical and Translational Science Institute (CTSI) Informatics.</a><br/>
        Cite NIH CTSA Award UM1TR004398 when using Tufts CTSI resources.<br/>
        Generated at $(Dates.now())
      </small>
    </td><td style="width: 28%; vertical-align: top; text-align: right;">
    $(isnothing(PROJECT_CODE) || isnothing(PROJECT_ID) ? "" :
      @htl("""Project#
        <a href="https://tuftsctsi.lightning.force.com/lightning/r/Project__c/$PROJECT_ID/view">
            $PROJECT_CODE</a><br /> """))
    $(isnothing(IRB_CODE) ? "" :
      @htl("<p>IRB Study# $(IRB_CODE)"))
    $(isnothing(PROJECT_SLUG) ? "" :
      @htl("""
         <a href="https://github.com/TuftsCTSI/ResearchRequests/tree/main/$PROJECT_SLUG/">
             $PROJECT_SLUG
         </a><br />"""))
  </div>
   """)
end

function NotebookHeader(TITLE=nothing)
    config = config_file()
    PROJECT_ID = config[:project_id]
    PROJECT_SLUG = config[:project_slug]
    PROJECT_CODE = config[:project_code]
    PROJECT_TITLE = config[:project_title]
    DESCRIPTION = config[:description]
    IRB = config[:irb_code]
    IRB_START_DATE = config[:irb_start_date]
    IRB_END_DATE = config[:irb_end_date]
  @htl("""
   <div style="overflow: auto; width: 100%; vertical-align: top;">
     <h1 style="display: inline-block; width: 88%; text-align: left; vertical-align: top;">
       $(TITLE)
     </h1>
     <div style="display: inline-block; width: 11%; text-align: right;
                 height: 100%; vertical-align: middle;">
        $(isnothing(PROJECT_CODE) ? "" : @htl("""Project #
          $(isnothing(PROJECT_ID) ? @htl("""<span>$PROJECT_CODE</span>""") : @htl("""
            <a style="text-decoration: underline dotted;"
               href="https://tuftsctsi.my.site.com/s/Project__c/$PROJECT_ID">$PROJECT_CODE</a>
        """))"""))
     </div>
   </div>
   <blockquote>
   $(isnothing(PROJECT_TITLE) ? "" :
     @htl("""<p style="font-variant: small-caps;">$PROJECT_TITLE</p>"""))
   $(isnothing(DESCRIPTION) ? "" :
     @htl("""<p>$DESCRIPTION</p>"""))
   </blockquote>
   $(if is_quality()
         nothing
     elseif is_discovery()
         @htl("""
             <p>
                 This cohort discovery is provided under IRB Protocol #11642,
                 <i>"Accelerating Clinical Trials - Multi-institutional cohort discovery"</i>,
                 permitting <i>"aggregate, obfuscated patient counts"</i>.
                 Counts below ten are indicated with the ≤ symbol.
                 Clinical data recorded between <b>$IRB_START_DATE</b> and <b>$IRB_END_DATE</b> are considered.
             </p>
         """)
     else
         timeframe = isnothing(IRB_START_DATE) || isnothing(IRB_END_DATE) ?
             @htl("<b>The IRB date range for this project has not been configured.</b>") :
             @htl("""<span>
                 The IRB approved timeframe for this study is <b>$IRB_START_DATE</b> to <b>$IRB_END_DATE</b>.
                 <i>Data must be recorded during this timeframe to be considered.</i>
                 If this study should consider any data outside this timeframe, please contact the IRB Office.</p>
                 </span>""")
         @htl("""<p>IRB Study # $(IRB). $timeframe</p>""")
   end)
   $(NotebookSidebar())
  """)

end

struct NotebookSidebar
end

function Base.show(io::IO, mime::MIME"text/html", ::NotebookSidebar)
    config = config_file()
    TITLE = config[:project_title]
    show(
        io,
        mime,
        @htl """
        <style>
          pluto-editor > header#pluto-nav, pluto-editor > footer {
            width: calc(100% - 1rem);
            margin-left: 1rem;
          }

          pluto-editor > main {
            max-width: unset;
            padding-right: 3rem;
            margin-right: 0;
            --prose-width: 800px;
          }

          pluto-output {
            min-width: min(100%, var(--prose-width));
            width: min-content;
            max-width: 100%;
            /*
            background: linear-gradient(to right, var(--pluto-output-bg-color) var(--prose-width), var(--pluto-output-bg-color) calc(100% - 10px), var(--footer-bg-color) 100%);
            */
          }

          pluto-output figure {
            margin-block-start: 0;
            margin-block-end: var(--pluto-cell-spacing);
            display: flex;
            flex-direction: column;
            align-items: center;
          }

          pluto-output figure:first-child {
            margin-block-start: 0;
          }

          pluto-output figure:last-child {
            margin-block-end: 0;
          }

          pluto-output figure img {
            max-width: unset;
          }

          pluto-output .dont-panic {
            display: none;
          }

          /*
          pluto-input {
            max-width: var(--prose-width);
          }

          pluto-logs-container {
            max-width: var(--prose-width);
          }

          pluto-runarea {
            right: max(calc(100% - var(--prose-width)), 0px);
          }
          */

          /* Adapted from https://github.com/JuliaPluto/PlutoUI.jl/blob/main/src/TableOfContents.jl */

          .trdw-sidebar {
            position: sticky;
            top: 0;
            margin-right: 1rem;
            padding-left: 1rem;
            width: 17rem;
            max-height: 100svh;
            flex: 0 0 auto;
            font-size: 14.5px;
            font-weight: 400;
            z-index: 40;
            overflow: auto;
            font-family: var(--system-ui-font-stack);
            background: var(--main-bg-color);
          }

          @media print {
            .trdw-sidebar {
              display: none;
            }
          }

          .trdw-sidebar-hidden {
            margin-left: -14.5rem;
          }

          .trdw-sidebar > nav {
            position: sticky;
            top: 1rem;
            margin: 1rem 0;
            padding: 1em;
            border-radius: 1rem;
            color: var(--pluto-output-color);
            background-color: var(--main-bg-color);
            --main-bg-color: #fafafa;
            --pluto-output-color: hsl(0, 0%, 36%);
            --pluto-output-h-color: hsl(0, 0%, 21%);
            --sidebar-li-active-bg: rgb(235, 235, 235);
            --icon-filter: unset;
          }

          @media (prefers-color-scheme: dark) {
            .trdw-sidebar > nav {
              --main-bg-color: #303030;
              --pluto-output-color: hsl(0, 0%, 90%);
              --pluto-output-h-color: hsl(0, 0%, 97%);
              --sidebar-li-active-bg: rgb(82, 82, 82);
              --icon-filter: invert(1);
            }
          }

          .trdw-sidebar > nav > header {
            display: flex;
            align-items: center;
            gap: 0.8rem;
            font-variant-caps: petite-caps;
            color: var(--pluto-output-h-color);
          }

          .trdw-sidebar.trdw-sidebar-hidden > nav > header {
            flex-direction: row-reverse;
            margin-bottom: 0;
          }

          .trdw-sidebar-toggle {
            cursor: pointer;
            display: flex;
          }

          .trdw-sidebar-toggle::before {
            content: "";
            display: inline-block;
            height: 1.2em;
            width: 1.2em;
            background-image: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTIiIGhlaWdodD0iNTEyIiB2aWV3Qm94PSIwIDAgNTEyIDUxMiI+PHRpdGxlPmlvbmljb25zLXY1LW88L3RpdGxlPjxsaW5lIHgxPSIxNjAiIHkxPSIxNDQiIHgyPSI0NDgiIHkyPSIxNDQiIHN0eWxlPSJmaWxsOm5vbmU7c3Ryb2tlOiMwMDA7c3Ryb2tlLWxpbmVjYXA6cm91bmQ7c3Ryb2tlLWxpbmVqb2luOnJvdW5kO3N0cm9rZS13aWR0aDozMnB4Ii8+PGxpbmUgeDE9IjE2MCIgeTE9IjI1NiIgeDI9IjQ0OCIgeTI9IjI1NiIgc3R5bGU9ImZpbGw6bm9uZTtzdHJva2U6IzAwMDtzdHJva2UtbGluZWNhcDpyb3VuZDtzdHJva2UtbGluZWpvaW46cm91bmQ7c3Ryb2tlLXdpZHRoOjMycHgiLz48bGluZSB4MT0iMTYwIiB5MT0iMzY4IiB4Mj0iNDQ4IiB5Mj0iMzY4IiBzdHlsZT0iZmlsbDpub25lO3N0cm9rZTojMDAwO3N0cm9rZS1saW5lY2FwOnJvdW5kO3N0cm9rZS1saW5lam9pbjpyb3VuZDtzdHJva2Utd2lkdGg6MzJweCIvPjxjaXJjbGUgY3g9IjgwIiBjeT0iMTQ0IiByPSIxNiIgc3R5bGU9ImZpbGw6bm9uZTtzdHJva2U6IzAwMDtzdHJva2UtbGluZWNhcDpyb3VuZDtzdHJva2UtbGluZWpvaW46cm91bmQ7c3Ryb2tlLXdpZHRoOjMycHgiLz48Y2lyY2xlIGN4PSI4MCIgY3k9IjI1NiIgcj0iMTYiIHN0eWxlPSJmaWxsOm5vbmU7c3Ryb2tlOiMwMDA7c3Ryb2tlLWxpbmVjYXA6cm91bmQ7c3Ryb2tlLWxpbmVqb2luOnJvdW5kO3N0cm9rZS13aWR0aDozMnB4Ii8+PGNpcmNsZSBjeD0iODAiIGN5PSIzNjgiIHI9IjE2IiBzdHlsZT0iZmlsbDpub25lO3N0cm9rZTojMDAwO3N0cm9rZS1saW5lY2FwOnJvdW5kO3N0cm9rZS1saW5lam9pbjpyb3VuZDtzdHJva2Utd2lkdGg6MzJweCIvPjwvc3ZnPg==");
            background-size: 1.2em;
            filter: var(--icon-filter);
          }

          .trdw-sidebar-toggle:hover::before {
            background-image: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTIiIGhlaWdodD0iNTEyIiB2aWV3Qm94PSIwIDAgNTEyIDUxMiI+PHRpdGxlPmlvbmljb25zLXY1LWE8L3RpdGxlPjxwb2x5bGluZSBwb2ludHM9IjI0NCA0MDAgMTAwIDI1NiAyNDQgMTEyIiBzdHlsZT0iZmlsbDpub25lO3N0cm9rZTojMDAwO3N0cm9rZS1saW5lY2FwOnJvdW5kO3N0cm9rZS1saW5lam9pbjpyb3VuZDtzdHJva2Utd2lkdGg6NDhweCIvPjxsaW5lIHgxPSIxMjAiIHkxPSIyNTYiIHgyPSI0MTIiIHkyPSIyNTYiIHN0eWxlPSJmaWxsOm5vbmU7c3Ryb2tlOiMwMDA7c3Ryb2tlLWxpbmVjYXA6cm91bmQ7c3Ryb2tlLWxpbmVqb2luOnJvdW5kO3N0cm9rZS13aWR0aDo0OHB4Ii8+PC9zdmc+");
          }

          .trdw-sidebar-hidden .trdw-sidebar-toggle:hover::before {
            background-image: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI1MTIiIGhlaWdodD0iNTEyIiB2aWV3Qm94PSIwIDAgNTEyIDUxMiI+PHRpdGxlPmlvbmljb25zLXY1LWE8L3RpdGxlPjxwb2x5bGluZSBwb2ludHM9IjI2OCAxMTIgNDEyIDI1NiAyNjggNDAwIiBzdHlsZT0iZmlsbDpub25lO3N0cm9rZTojMDAwO3N0cm9rZS1saW5lY2FwOnJvdW5kO3N0cm9rZS1saW5lam9pbjpyb3VuZDtzdHJva2Utd2lkdGg6NDhweCIvPjxsaW5lIHgxPSIzOTIiIHkxPSIyNTYiIHgyPSIxMDAiIHkyPSIyNTYiIHN0eWxlPSJmaWxsOm5vbmU7c3Ryb2tlOiMwMDA7c3Ryb2tlLWxpbmVjYXA6cm91bmQ7c3Ryb2tlLWxpbmVqb2luOnJvdW5kO3N0cm9rZS13aWR0aDo0OHB4Ii8+PC9zdmc+");
          }

          .trdw-sidebar > nav > section {
            padding-top: 1em;
            margin-top: 1em;
            margin-bottom: 1em;
            border-top: 2px dotted var(--pluto-output-color);
          }

          .trdw-sidebar-hidden > nav > section {
            display: none;
          }

          .trdw-sidebar > nav > section > ul {
            list-style: none;
            padding: 0;
            margin: 0;
          }

          .trdw-sidebar > nav > section > ul > li {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            border-radius: 0.5em;
          }

          .trdw-sidebar > nav > section > ul > li.trdw-sidebar-link-current {
            background: var(--sidebar-li-active-bg);
          }

          .trdw-sidebar a {
            text-decoration: none;
            color: var(--pluto-output-color);
          }

          .trdw-sidebar a:hover {
            color: var(--pluto-output-h-color);
          }

          .trdw-sidebar-link-H1 {
            margin-top: 0.5em;
            font-weight: 500;
            padding: 0 10px;
          }

          .trdw-sidebar-link-H2 {
            padding: 0 10px 0 20px;
          }

          .trdw-sidebar-link-H3 {
            padding: 0 10px 0 30px;
          }

          .trdw-sidebar > nav > section > p {
            padding: 0;
            margin: 0.5em 10px;
            font-style: italic;
          }
        </style>

        <aside class="trdw-sidebar" style="display: none">
          <nav>
            <header>
              <span class="trdw-sidebar-toggle"></span>
              <span class="trdw-sidebar-title">$TITLE</span>
            </header>
            <section><p>Loading Table of Contents&hellip;</p></section>
          </nav>
          <script>
            const disableFetch = !window.pluto_disable_ui
            const cellNode = currentScript.closest("pluto-cell")
            const notebookNode = currentScript.closest("pluto-notebook")
            const editorNode = currentScript.closest("pluto-editor")
            const sidebarNode = currentScript.parentElement
            const toggleNode = sidebarNode.querySelector(".trdw-sidebar-toggle")
            editorNode.parentElement.prepend(sidebarNode)
            sidebarNode.style.display = null

            const onToggle = () => {
              sidebarNode.classList.toggle("trdw-sidebar-hidden")
            }
            toggleNode.addEventListener("click", onToggle)

            const fetchNotebooks = async () => {
              try {
                if (disableFetch) {
                  throw(Error("In development, links to other notebooks are not available"))
                }
                const response = await fetch("pluto_export.json")
                if (!response.ok) {
                  throw(Error(response.statusText))
                }
                const json = await response.json()
                let ns = Object.values(json.notebooks)
                ns.sort((a, b) => {
                  const a_order = Number(a.frontmatter?.order)
                  const b_order = Number(b.frontmatter?.order)
                  if (isNaN(a_order) && isNaN(b_order)) {
                    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0
                  }
                  else if (isNaN(a_order)) {
                    return 1
                  }
                  else if (isNaN(b_order)) {
                    return -1
                  }
                  else {
                    return a_order - b_order
                  }
                })
                return [ns, null]
              }
              catch (err) {
                return [[], err]
              }
            }

            const [notebooks, notebooksError] = await fetchNotebooks()

            let currentLink = null;
            const hToLinkMap = new Map()
            const hIntersectingSet = new Set()
            const hObserver = new IntersectionObserver((entries) => {
              for (const entry of entries) {
                if (entry.isIntersecting) {
                  hIntersectingSet.add(entry.target)
                }
                else {
                  hIntersectingSet.delete(entry.target)
                }
              }
              let nextCurrentLink = null
              for (const [h, liNode] of hToLinkMap) {
                if (!h || hIntersectingSet.has(h)) {
                  nextCurrentLink = liNode
                }
                if (nextCurrentLink !== currentLink) {
                  if (currentLink) {
                    currentLink.classList.remove("trdw-sidebar-link-current")
                  }
                  if (nextCurrentLink) {
                    nextCurrentLink.classList.add("trdw-sidebar-link-current")
                  }
                  currentLink = nextCurrentLink
                }
              }
            }, { rootMargin: "1000000px 0px -75% 0px" })

            const makeInternalLinks = () => {
              const thisNotebook = cellNode._internal_pluto_actions.get_notebook()
              const links = []
              const title1 = thisNotebook?.metadata?.frontmatter?.title || document.title
              hObserver.disconnect()
              hToLinkMap.clear()
              hIntersectingSet.clear()
              currentLink = null
              if (title1) {
                const aNode1 = document.createElement("a")
                const id1 = notebookNode.querySelector("pluto-cell").id
                aNode1.href = `#\${id1}`
                aNode1.innerText = aNode1.title = title1
                const liNode1 = html`<li class="trdw-sidebar-link-H1 trdw-sidebar-link-internal trdw-sidebar-link-current">\${aNode1}</li>`
                links.push(liNode1)
                hToLinkMap.set(null, liNode1)
                currentLink = liNode1
              }
              let hs = Array.from(notebookNode.querySelectorAll("pluto-cell h1, pluto-cell h2, pluto-cell h3"))
              if (hs.length > 0 && hs[0].tagName == "H1" && links.length > 0) {
                hs.shift()
              }
              for (const h of hs) {
                const aNode = document.createElement("a")
                const id = h.closest("pluto-cell").id
                aNode.href = `#\${id}`
                aNode.title = h.innerText
                aNode.innerHTML = h.innerHTML
                const liNode = html`<li class="trdw-sidebar-link-\${h.tagName} trdw-sidebar-link-internal">\${aNode}</li>`
                links.push(liNode)
                hToLinkMap.set(h, liNode)
                hObserver.observe(h)
              }
              return links
            }

            const makeLinks = () => {
              const currentPath = window.location.pathname.split("/").pop() || "index.html"
              let links = []
              let hasInternal = false
              for (const n of notebooks) {
                if (n.html_path == currentPath) {
                  links = links.concat(makeInternalLinks())
                  hasInternal = true
                }
                else {
                  const title = n.frontmatter.title ?? n.id.replace(/\\.jl\$/, "")
                  const aNode = document.createElement("a")
                  aNode.href = n.html_path == "index.html" ? "./" : n.html_path
                  aNode.innerText = aNode.title = title
                  links.push(html`<li class="trdw-sidebar-link-H1">\${aNode}</li>`)
                }
              }
              if (!hasInternal) {
                links = links.concat(makeInternalLinks())
              }
              return links
            }

            const updateLinks = () => {
              const sectionNode = document.createElement("section")
              const links = makeLinks()
              if (links.length > 0) {
                sectionNode.append(html`<ul>\${links}</ul>`)
              }
              if (notebooksError) {
                const pNode = document.createElement("p")
                pNode.innerText = notebooksError.message
                sectionNode.append(pNode)
              }
              sidebarNode.querySelector("section").replaceWith(sectionNode)
            }

            updateLinks()

            const cellObservers = []

            const updateCellObservers = () => {
              cellObservers.forEach((o) => o.disconnect())
              cellObservers.length = 0
              for (const node of notebookNode.getElementsByTagName("pluto-cell")) {
                const o = new MutationObserver(updateLinks)
                o.observe(node, { attributeFilter: ["class"] })
                cellObservers.push(o)
              }
            }

            updateCellObservers()

            const notebookObserver = new MutationObserver(() => {
              updateLinks()
              updateCellObservers()
            })

            notebookObserver.observe(notebookNode, { childList: true })

            invalidation.then(() => {
              hObserver.disconnect()
              cellObservers.forEach((o) => o.disconnect())
              notebookObserver.disconnect()
              toggleNode.removeEventListener("click", onToggle)
              sidebarNode.remove()
            })
          </script>
        </aside>
        """)
end

struct NotebookList
end

function Base.show(io::IO, mime::MIME"text/html", ::NotebookList)
    show(
        io,
        mime,
        @htl """
        <style>
          .trdw-notebook-list {
            font-family: var(--system-ui-font-stack);
            padding: 1rem;
            margin: 1rem 0;
            border-radius: 1rem;
            color: var(--pluto-output-color);
            background-color: var(--main-bg-color);
            --main-bg-color: #fafafa;
            --pluto-output-color: hsl(0, 0%, 36%);
            --pluto-output-h-color: hsl(0, 0%, 21%);
            --sidebar-li-active-bg: rgb(235, 235, 235);
          }

          @media (prefers-color-scheme: dark) {
            .trdw-notebook-list {
              --main-bg-color: #303030;
              --pluto-output-color: hsl(0, 0%, 90%);
              --pluto-output-h-color: hsl(0, 0%, 97%);
              --sidebar-li-active-bg: rgb(82, 82, 82);
            }
          }

          .trdw-notebook-list > section > ol {
            font-weight: 500;
            margin: 0;
          }

          .trdw-notebook-list > section > ol > li {
            margin: 0.5em 0;
          }

          .trdw-notebook-list li a {
            display: inline-flex;
            flex-direction: column;
            text-decoration: none;
            color: var(--pluto-output-color);
          }

          .trdw-notebook-list li a:hover {
            color: var(--pluto-output-h-color);
          }

          .trdw-notebook-list li small {
            font-weight: 400;
          }

          .trdw-notebook-list > section > p {
            padding: 0;
            margin: 0.5em 10px;
            font-style: italic;
          }
        </style>

        <nav class="trdw-notebook-list">
          <section><p>Loading notebook list&hellip;</p></section>
          <script>
            const disableFetch = !window.pluto_disable_ui
            const navNode = currentScript.closest("nav")

            const fetchNotebooks = async () => {
              try {
                if (disableFetch) {
                  throw(Error("In development, links to other notebooks are not available"))
                }
                const response = await fetch("pluto_export.json")
                if (!response.ok) {
                  throw(Error(response.statusText))
                }
                const json = await response.json()
                let ns = Object.values(json.notebooks)
                ns.sort((a, b) => {
                  const a_order = Number(a.frontmatter?.order)
                  const b_order = Number(b.frontmatter?.order)
                  if (isNaN(a_order) && isNaN(b_order)) {
                    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0
                  }
                  else if (isNaN(a_order)) {
                    return 1
                  }
                  else if (isNaN(b_order)) {
                    return -1
                  }
                  else {
                    return a_order - b_order
                  }
                })
                return [ns, null]
              }
              catch (err) {
                return [[], err]
              }
            }

            const [notebooks, notebooksError] = await fetchNotebooks()

            const makeLinks = () => {
              const currentPath = window.location.pathname.split("/").pop() || "index.html"
              let links = []
              for (const n of notebooks) {
                const title = n.frontmatter.title ?? n.id.replace(/\\.jl\$/, "")
                const description = n.frontmatter.description
                const current = n.html_path == currentPath
                const aNode = document.createElement("a")
                aNode.href = n.html_path == "index.html" ? "./" : n.html_path
                aNode.innerText = aNode.title = title
                if (current) {
                  aNode.classList.add("trdw-notebook-list-current")
                }
                if (description) {
                  const smallNode = document.createElement("small")
                  smallNode.innerText = description
                  aNode.append(smallNode)
                }
                links.push(html`<li>\${aNode}</li>`)
              }
              return links
            }

            const sectionNode = document.createElement("section")
            const links = makeLinks()
            if (links.length > 0) {
              sectionNode.append(html`<ol>\${links}</ol>`)
            }
            if (notebooksError) {
              const pNode = document.createElement("p")
              pNode.innerText = notebooksError.message
              sectionNode.append(pNode)
            }
            navNode.querySelector("section").replaceWith(sectionNode)
          </script>
        </nav>
        """)
end

struct DownloadList
end

function Base.show(io::IO, mime::MIME"text/html", ::DownloadList)
    show(
        io,
        mime,
        @htl """
        <style>
          .trdw-download-list {
            font-family: var(--system-ui-font-stack);
            padding: 1rem;
            margin: 1rem 0;
            border-radius: 1rem;
            color: var(--pluto-output-color);
            background-color: var(--main-bg-color);
            --main-bg-color: #fafafa;
            --pluto-output-color: hsl(0, 0%, 36%);
            --pluto-output-h-color: hsl(0, 0%, 21%);
            --sidebar-li-active-bg: rgb(235, 235, 235);
          }

          @media (prefers-color-scheme: dark) {
            .trdw-download-list {
              --main-bg-color: #303030;
              --pluto-output-color: hsl(0, 0%, 90%);
              --pluto-output-h-color: hsl(0, 0%, 97%);
              --sidebar-li-active-bg: rgb(82, 82, 82);
            }
          }

          .trdw-download-list > section > table {
            margin: 0.5em 10px;
          }

          .trdw-download-list > section > table > tbody > tr > td {
            font-family: inherit;
            font-size: inherit;
            font-variant-ligatures: inherit;
          }

          .trdw-download-list a {
            text-decoration: none;
            color: var(--pluto-output-color);
          }

          .trdw-download-list a:hover {
            color: var(--pluto-output-h-color);
          }

          .trdw-download-list > section > p {
            padding: 0;
            margin: 0.5em 10px;
            font-style: italic;
          }
        </style>

        <nav class="trdw-download-list">
          <section><p>Loading download list&hellip;</p></section>
          <script>
            const disableFetch = !window.pluto_disable_ui
            const navNode = currentScript.closest("nav")

            const fetchDownloads = async () => {
              try {
                if (disableFetch) {
                  throw(Error("In development, downloads are not available"))
                }
                const response = await fetch("download.json")
                if (!response.ok) {
                  throw(Error(response.status == 404 ? "No files to download" : response.statusText))
                }
                const json = await response.json()
                return [json.files, null]
              }
              catch (err) {
                return [[], err]
              }
            }

            const [downloads, downloadsError] = await fetchDownloads()

            const formatFileSize = (size) => {
              if (size == 1) {
                return '1 byte'
              }
              else if (size < 1024) {
                return `\${size} bytes`
              }
              else if (1024 <= size && size < 1048576) {
                return `\${(size / 1024).toFixed(1)} kB`
              }
              else {
                return `\${(size / 1048576).toFixed(1)} MB`
              }
            }

            const makeRows = () => {
              let rows = []
              for (const d of downloads) {
                const aNode = document.createElement("a")
                aNode.href = `./download/\${d.name}`
                aNode.innerText = aNode.title = d.name
                rows.push(html`<tr><td>\${aNode}</td><td>\${formatFileSize(d.size)}</td></tr>`)
              }
              return rows
            }

            const sectionNode = document.createElement("section")
            const rows = makeRows()
            if (rows.length > 0) {
              sectionNode.append(html`<table><thead><tr><th>Name</th><th>Size</th></tr></thead><tbody>\${rows}</tbody></table>`)
            }
            if (downloadsError) {
              const pNode = document.createElement("p")
              pNode.innerText = downloadsError.message
              sectionNode.append(pNode)
            }
            navNode.querySelector("section").replaceWith(sectionNode)
          </script>
        </nav>
        """)
end

struct VersionList
end

function Base.show(io::IO, mime::MIME"text/html", ::VersionList)
    show(
        io,
        mime,
        @htl """
        <style>
          .trdw-version-list {
            font-family: var(--system-ui-font-stack);
            padding: 1rem;
            margin: 1rem 0;
            border-radius: 1rem;
            color: var(--pluto-output-color);
            background-color: var(--main-bg-color);
            --main-bg-color: #fafafa;
            --pluto-output-color: hsl(0, 0%, 36%);
            --pluto-output-h-color: hsl(0, 0%, 21%);
            --sidebar-li-active-bg: rgb(235, 235, 235);
          }

          @media (prefers-color-scheme: dark) {
            .trdw-version-list {
              --main-bg-color: #303030;
              --pluto-output-color: hsl(0, 0%, 90%);
              --pluto-output-h-color: hsl(0, 0%, 97%);
              --sidebar-li-active-bg: rgb(82, 82, 82);
            }
          }

          .trdw-version-list > section > ul {
            list-style: none;
            padding: 0;
            margin: 0;
            column-width: 12rem;
            column-count: auto;
          }

          .trdw-version-list > section > ul > li {
            padding: 0.5em 10px;
            border-radius: 0.5em;
            font-weight: 500;
          }

          .trdw-version-list li:has(.trdw-version-list-current) {
            background: var(--sidebar-li-active-bg);
          }

          .trdw-version-list a {
            display: inline-flex;
            flex-direction: column;
            text-decoration: none;
            color: var(--pluto-output-color);
          }

          .trdw-version-list a:hover {
            color: var(--pluto-output-h-color);
          }

          .trdw-version-list li small {
            font-weight: 400;
          }

          .trdw-version-list > section > p {
            padding: 0;
            margin: 0.5em 10px;
            font-style: italic;
          }
        </style>

        <nav class="trdw-version-list">
          <section><p>Loading version list&hellip;</p></section>
          <script>
            const disableFetch = !window.pluto_disable_ui
            const navNode = currentScript.closest("nav")
            const currentTag = window.location.pathname.match(/([^/]*)\\/[^/]*\$/)[1]

            const fetchVersions = async () => {
              try {
                if (disableFetch) {
                  throw(Error("In development, other versions are not available"))
                }
                const response = await fetch("../versions.json")
                if (!response.ok) {
                  throw(Error(response.statusText))
                }
                const json = await response.json()
                const versions = json.versions
                versions.reverse()
                if (versions.length == 1 && versions[0].tag == currentTag) {
                  throw(Error("No other versions are available"))
                }
                return [versions, null]
              }
              catch (err) {
                return [[], err]
              }
            }

            const [versions, versionsError] = await fetchVersions()

            const formatTag = (tag) => {
              const matches = tag.match(/^(\\d\\d\\d\\d)(\\d\\d)(\\d\\d)T(\\d\\d)(\\d\\d)\$/)
              if (!matches) {
                return tag
              }
              return `\${matches[1]}-\${matches[2]}-\${matches[3]} \${matches[4]}:\${matches[5]}`
            }

            const makeLinks = () => {
              let links = []
              for (const version of versions) {
                const aNode = document.createElement("a")
                aNode.href = `../\${version.tag}/`
                aNode.innerText = aNode.title = formatTag(version.tag)
                if (version.tag == currentTag) {
                  aNode.classList.add("trdw-version-list-current")
                }
                const description = version.commit?.subject
                if (description) {
                  const smallNode = document.createElement("small")
                  smallNode.innerText = description
                  aNode.append(smallNode)
                }
                links.push(html`<li>\${aNode}</li>`)
              }
              return links
            }

            const sectionNode = document.createElement("section")
            const links = makeLinks()
            if (links.length > 0) {
              sectionNode.append(html`<ul>\${links}</ul>`)
            }
            if (versionsError) {
              const pNode = document.createElement("p")
              pNode.innerText = versionsError.message
              sectionNode.append(pNode)
            }
            navNode.querySelector("section").replaceWith(sectionNode)
          </script>
        </nav>
        """)
end
