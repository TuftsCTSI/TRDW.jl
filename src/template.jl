function NotebookFooter()
    config = configuration()
    SFID = config[:case_slug]
    CASE = config[:case_code]
    IRB = config[:irb_code]
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
    $(isnothing(CASE) ? "" : @htl("""Service Request#
      $(isnothing(SFID) ? @htl("""<span>$CASE</span>""") : @htl("""
        <a href="https://tuftsctsi.lightning.force.com/lightning/r/Case/$SFID/view">$CASE</a>
        """))
      <br />"""))
    $(is_discovery() ? "" : @htl("<p>IRB Study# $(IRB)"))
  </div>
   """)
end

function NotebookHeader(TITLE=nothing)
    config = configuration()
    SFID = config[:case_slug]
    CASE = config[:case_code]
    TITLE = something(TITLE, config[:case_title])
    NOTE = config[:project_title]
    IRB = config[:irb_code]
    IRB_START_DATE = config[:irb_start_date]
    IRB_END_DATE = config[:irb_end_date]
  @htl("""
   <!-- wide notebooks -->
   <style>
           main {
               margin: 0 auto;
               max-width: 2000px;
               padding-right: 50px;
           }
   </style>
   <div style="overflow: auto; width: 100%; vertical-align: top;">
     <h1 style="display: inline-block; width: 88%; text-align: left; vertical-align: top;">
       $(TITLE)
     </h1>
     <div style="display: inline-block; width: 11%; text-align: right;
                 height: 100%; vertical-align: middle;">
       $(isnothing(CASE) ? "" : @htl("""Service Request#
          $(isnothing(SFID) ? @htl("""<span>$CASE</span>""") : @htl("""
            <a style="text-decoration: underline dotted;"
               href="https://tuftsctsi.my.site.com/s/case/$SFID ">$CASE</a>
        """))"""))
     </div>
   </div>
   $(isnothing(NOTE) ? "" :
     @htl("""<p style="font-style: italic; font-size: 21px;">$NOTE</p>"""))
   $(if is_discovery()
        @htl("""
            <p>
                This cohort discovery is provided under IRB Protocol #11642,
                <i>"Accelerating Clinical Trials - Multi-institutional cohort discovery"</i>,
                permitting <i>"aggregate, obfuscated patient counts"</i>.
                Counts below ten are indicated with the ≤ symbol.
            </p>
            $(isnothing(IRB_START_DATE) ? "" : @htl("""
                <span>IRB Date Range: ($IRB_START_DATE to $IRB_END_DATE)</span>
            """))
        """)
     else
        @htl("""
            <p>IRB Study # $(IRB)
            $(isnothing(IRB_START_DATE) ? "" : @htl("($IRB_START_DATE to $IRB_END_DATE)"))
            </p>
        """)
     end)
   $(PlutoUI.TableOfContents())
  """)
end
