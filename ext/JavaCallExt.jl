module JavaCallExt

using TRDW

using JavaCall
using Markdown
using Pkg.Artifacts
using Tables
using Dates

const ByteArrayInputStream = @jimport java.io.ByteArrayInputStream
const ByteArrayOutputStream = @jimport java.io.ByteArrayOutputStream
const CellStyle = @jimport org.apache.poi.ss.usermodel.CellStyle
const CreationHelper = @jimport org.apache.poi.ss.usermodel.CreationHelper
const DataFormat = @jimport org.apache.poi.ss.usermodel.DataFormat
const EncryptionInfo = @jimport org.apache.poi.poifs.crypt.EncryptionInfo
const EncryptionMode = @jimport org.apache.poi.poifs.crypt.EncryptionMode
const Encryptor = @jimport org.apache.poi.poifs.crypt.Encryptor
const File = @jimport java.io.File
const FileOutputStream = @jimport java.io.FileOutputStream
const InputStream = @jimport java.io.InputStream
const IOUtils = @jimport org.apache.poi.util.IOUtils
const LocalDateTime = @jimport java.time.LocalDateTime
const OPCPackage = @jimport org.apache.poi.openxml4j.opc.OPCPackage
const OutputStream = @jimport java.io.OutputStream
const PackageAccess = @jimport org.apache.poi.openxml4j.opc.PackageAccess
const POIFSFileSystem = @jimport org.apache.poi.poifs.filesystem.POIFSFileSystem
const SXSSFCell = @jimport org.apache.poi.xssf.streaming.SXSSFCell
const SXSSFRow = @jimport org.apache.poi.xssf.streaming.SXSSFRow
const SXSSFSheet = @jimport org.apache.poi.xssf.streaming.SXSSFSheet
const SXSSFWorkbook = @jimport org.apache.poi.xssf.streaming.SXSSFWorkbook

function TRDW.XLSX.write(file, table; password = nothing)
    TRDW.XLSX.write(file, ["Sheet1" => table]; password)
end

function TRDW.XLSX.write(file, sheets::AbstractVector{<:Pair{<:AbstractString}}; password = nothing)
    jcall(IOUtils, "setByteArrayMaxOverride", Nothing, (jint,), typemax(Int32))
    TRDW.XLSX.validate_sheet_names([first(p) for p in sheets])
    workbook = SXSSFWorkbook(())
    try
        creation_helper = jcall(workbook, "getCreationHelper", CreationHelper, ())
        date_format = jcall(creation_helper, "createDataFormat", DataFormat, ())
        date_format_idx = jcall(date_format, "getFormat", jshort, (JString,), "yyyy-mm-dd")
        datetime_format = jcall(creation_helper, "createDataFormat", DataFormat, ())
        datetime_format_idx = jcall(datetime_format, "getFormat", jshort, (JString,), "yyyy-mm-dd hh:mm:ss")
        date_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
        jcall(date_cell_style, "setDataFormat", Nothing, (jshort,), date_format_idx)
        datetime_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
        jcall(datetime_cell_style, "setDataFormat", Nothing, (jshort,), datetime_format_idx)
        wrap_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
        jcall(wrap_cell_style, "setWrapText", Nothing, (jboolean,), true)
        control_char_locations = Tuple{String, Symbol, Int, Vector{Char}}[]
        for (sheet_name, table) in sheets
            sheet = jcall(workbook, "createSheet", SXSSFSheet, (JString,), sheet_name)
            jcall(sheet, "trackAllColumnsForAutoSizing", Nothing, ())
            sch = Tables.schema(table)
            cols = Tables.columnnames(table)
            types = sch.types
            for (i, (c, t)) in enumerate(zip(cols, types))
                nt = Base.nonmissingtype(t)
                if nt <: Dates.Date
                    jcall(sheet, "setDefaultColumnStyle", Nothing, (jint, CellStyle), i-1, date_cell_style)
                elseif nt <: Dates.DateTime
                    jcall(sheet, "setDefaultColumnStyle", Nothing, (jint, CellStyle), i-1, datetime_cell_style)
                end
            end
            row = jcall(sheet, "createRow", SXSSFRow, (jint,), 0)
            for (i, c) in enumerate(cols)
                cell = jcall(row, "createCell", SXSSFCell, (jint,), i-1)
                header = TRDW.XLSX.decode_funsql_label(string(c))
                TRDW.XLSX.check_javacall_compatible(header; context = "column header \"$header\"")
                TRDW.XLSX.check_cell_length(header; context = "column header \"$header\"")
                header = TRDW.XLSX.sanitize_for_xlsx(header)
                jcall(cell, "setCellValue", Nothing, (JString,), header)
            end
            for (k, r) in enumerate(Tables.rows(table))
                row = jcall(sheet, "createRow", SXSSFRow, (jint,), k)
                for (i, c) in enumerate(Tables.columnnames(r))
                    val = Tables.getcolumn(r, c)
                    cell = jcall(row, "createCell", SXSSFCell, (jint,), i-1)
                    if val === missing
                    elseif val isa Bool
                        jcall(cell, "setCellValue", Nothing, (jboolean,), val)
                    elseif val isa Dates.Date
                        datetime = jcall(LocalDateTime, "of", LocalDateTime, (jint, jint, jint, jint, jint), year(val), month(val), day(val), 0, 0)
                        jcall(cell, "setCellValue", Nothing, (LocalDateTime,), datetime)
                    elseif val isa Dates.DateTime
                        datetime = jcall(LocalDateTime, "of", LocalDateTime, (jint, jint, jint, jint, jint, jint, jint), year(val), month(val), day(val), hour(val), minute(val), second(val), millisecond(val) * 1000000)
                        jcall(cell, "setCellValue", Nothing, (LocalDateTime,), datetime)
                    elseif val isa Number
                        jcall(cell, "setCellValue", Nothing, (jdouble,), val)
                    else
                        raw = string(val)
                        ctx = "column \"$(string(c))\", row $k"
                        TRDW.XLSX.check_javacall_compatible(raw; context = ctx)
                        TRDW.XLSX.check_cell_length(raw; context = ctx)
                        str = TRDW.XLSX.sanitize_for_xlsx(raw)
                        if str !== raw
                            chars = TRDW.XLSX.find_invalid_control_chars(raw)
                            push!(control_char_locations, (sheet_name, c, k, chars))
                        end
                        if contains(str, '\n')
                            jcall(cell, "setCellStyle", Nothing, (CellStyle,), wrap_cell_style)
                        end
                        jcall(cell, "setCellValue", Nothing, (JString,), str)
                    end
                end
            end
            for i in 1:length(cols)
                jcall(sheet, "autoSizeColumn", Nothing, (jint,), i-1)
                width = jcall(sheet, "getColumnWidth", jint, (jint,), i-1)
                width = round(Int, width * TRDW.XLSX.AUTOSIZE_CORRECTION_FACTOR)
                if width > TRDW.XLSX.MAX_COLUMN_WIDTH
                    width = TRDW.XLSX.DEFAULT_COLUMN_WIDTH
                end
                jcall(sheet, "setColumnWidth", Nothing, (jint, jint), i-1, width)
            end
        end
        if !isempty(control_char_locations)
            n = length(control_char_locations)
            examples = control_char_locations[1:min(3, n)]
            detail = join(["sheet \"$(s)\", column \"$(col)\", row $(r) ($(TRDW.XLSX.describe_codepoints(chars)))" for (s, col, r, chars) in examples], "; ")
            suffix = n > 3 ? " (and $(n - 3) more)" : ""
            @warn "Control characters were replaced with spaces: $detail$suffix"
        end
        if password !== nothing
            buffer = ByteArrayOutputStream(())
            bytes = try
                jcall(workbook, "write", Nothing, (OutputStream,), buffer)
                jcall(buffer, "toByteArray", Vector{jbyte}, ())
            finally
                jcall(buffer, "close", Nothing, ())
            end
            filesystem = POIFSFileSystem(())
            try
                agile_encryption_mode = jfield(EncryptionMode, "agile", EncryptionMode)
                encryption_info = EncryptionInfo((EncryptionMode,), agile_encryption_mode)
                encryptor = jcall(encryption_info, "getEncryptor", Encryptor, ())
                jcall(encryptor, "confirmPassword", Nothing, (JString,), password)
                input_stream = ByteArrayInputStream((Vector{jbyte},), bytes)
                try
                    package = jcall(OPCPackage, "open", OPCPackage, (InputStream,), input_stream)
                    try
                        encrypted_stream = jcall(encryptor, "getDataStream", OutputStream, (POIFSFileSystem,), filesystem)
                        try
                            jcall(package, "save", Nothing, (OutputStream,), encrypted_stream)
                        finally
                            jcall(encrypted_stream, "close", Nothing, ())
                        end
                    finally
                        jcall(package, "close", Nothing, ())
                    end
                finally
                    jcall(input_stream, "close", Nothing, ())
                end
                file_output_stream = FileOutputStream((JString,), file)
                try
                    jcall(filesystem, "writeFilesystem", Nothing, (OutputStream,), file_output_stream)
                finally
                    jcall(file_output_stream, "close", Nothing, ())
                end
            finally
                jcall(filesystem, "close", Nothing, ())
            end
        else
            file_output_stream = FileOutputStream((JString,), file)
            try
                jcall(workbook, "write", Nothing, (OutputStream,), file_output_stream)
            finally
                jcall(file_output_stream, "close", Nothing, ())
            end
        end
    finally
        success = jcall(workbook, "dispose", jboolean, ())
        success || @warn "SXSSFWorkbook.dispose() failed; temporary files may remain in $(tempdir())"
    end
    nothing
end

const CohortExpressionQueryBuilder = @jimport org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder
const BuildExpressionQueryOptions = @jimport org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder$BuildExpressionQueryOptions
const MarkdownRender = @jimport org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender
const SqlRender = @jimport org.ohdsi.sql.SqlRender
const SqlTranslate = @jimport org.ohdsi.sql.SqlTranslate
const SqlSplit = @jimport org.ohdsi.sql.SqlSplit

function TRDW.OHDSI.cohort_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(mr, "renderCohort", JString, (JString,), str) |> Markdown.parse
end

function TRDW.OHDSI.concept_set_list_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(mr, "renderConceptSetList", JString, (JString,), str) |> Markdown.parse
end

function TRDW.OHDSI.concept_set_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(mr, "renderConceptSet", JString, (JString,), str) |> Markdown.parse
end

function TRDW.OHDSI.cohort_definition_to_sql_template(str)
    builder = CohortExpressionQueryBuilder(())
    jcall(builder, "buildExpressionQuery", JString, (JString, BuildExpressionQueryOptions), str, nothing)
end

function TRDW.OHDSI.render_sql(template, params = (;))
    jcall(SqlRender, "renderSql", JString, (JString, Vector{JString}, Vector{JString}),
        template,
        collect(String, string.(keys(params))),
        collect(String, string.(values(params))))
end

function TRDW.OHDSI.translate_sql(sql; dialect = "spark", session_id = nothing, temp_emulation_schema = nothing)
    jcall(SqlTranslate, "translateSql", JString, (JString, JString, JString, JString),
        sql, dialect,
        session_id !== nothing ? string(session_id) : nothing,
        temp_emulation_schema !== nothing ? string(temp_emulation_schema) : nothing)
end

function TRDW.OHDSI.split_sql(sql)
    v = jcall(SqlSplit, "splitSql", Vector{JString}, (JString,), sql)
    map(JavaCall.unsafe_string, v)
end

function __init__()
    JavaCall.addClassPath(joinpath(artifact"csv2xlsx", "*"))
    JavaCall.addClassPath(joinpath(artifact"CirceR", "CirceR-1.3.2/inst/java/*"))
    JavaCall.addClassPath(joinpath(artifact"SqlRender", "SqlRender-1.16.1/inst/java/*"))
end

end #module JavaCallExt

