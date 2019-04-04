package com.zq.simpledatax.plugin.reader.excelreader;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ExcelUtil.class);
	public static List<Map<String, String>> parse2List(String filePath,List<String> cols,List<DataSheet> dataSheets) throws IOException {
		LOG.info("开始解析excel表格！");
		Workbook workbook = getWorkbook(filePath);
        List<Map<String, String>> result = new ArrayList<Map<String, String>>();
        for(DataSheet dataSheet:dataSheets) {
        	int sheetIndex = dataSheet.getSheetIndex()-1;
        	int startNum = dataSheet.getSkipRowNum();
        	Sheet sheet = workbook.getSheetAt(sheetIndex);
            if (sheet == null) {
            	continue;     	
            }
            for (int j = startNum; j < (sheet.getLastRowNum() + 1); j++) {
            	Row row = sheet.getRow(j);
                Map<String, String> map = new HashMap<String, String>();
                for (int y = 0; y < cols.size(); y++) {
                	Cell cell  = row.getCell(y);
                	String value = getStringValueFromCell(cell);
                    map.put(cols.get(y), value);
                }
                result.add(map);
            }
        }
        LOG.info("结束解析excel表格！");
	    return result;		
	}

	private static Workbook getWorkbook(String filePath) throws IOException {
		String extension = filePath.substring(filePath.lastIndexOf(".")+1);
		FileInputStream fis = new FileInputStream(filePath);
		Workbook workbook = null;
		if(extension.equalsIgnoreCase("xlsx")) {
			workbook = new XSSFWorkbook(fis);
		}else if(extension.equalsIgnoreCase("xls")) {
			workbook = new HSSFWorkbook(fis);
		}
		return workbook;
	}
	
    public static String getStringValueFromCell(Cell cell) {
        SimpleDateFormat sFormat = new SimpleDateFormat("yyyy-MM-dd");
        DecimalFormat decimalFormat = new DecimalFormat("#.#");
        String cellValue = "";
        if(cell == null) {
            return cellValue;
        }
        else if(cell.getCellType() == Cell.CELL_TYPE_STRING) {
            cellValue = cell.getStringCellValue();
        }

        else if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
            if(HSSFDateUtil.isCellDateFormatted(cell)) {
                double d = cell.getNumericCellValue();
                Date date = HSSFDateUtil.getJavaDate(d);
                cellValue = sFormat.format(date);
            }
            else {                
                cellValue = decimalFormat.format((cell.getNumericCellValue()));
            }
        }
        else if(cell.getCellType() == Cell.CELL_TYPE_BLANK) {
            cellValue = "";
        }
        else if(cell.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
            cellValue = String.valueOf(cell.getBooleanCellValue());
        }
        else if(cell.getCellType() == Cell.CELL_TYPE_ERROR) {
            cellValue = "";
        }
        else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
            cellValue = cell.getCellFormula().toString();
        }
        return cellValue;
    }
    
}
