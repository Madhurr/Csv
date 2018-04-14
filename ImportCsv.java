package com.madhur.csv;

 

import java.io.FileReader;
import java.sql.Connection;

import java.sql.PreparedStatement;

import java.sql.Statement;

 

import com.opencsv.CSVReader;

 

public class ImportCsv

{

    public static void main(String[] args)

    {

            readCsv();

            readCsvUsingLoad();

    }

 

    private static void readCsv()

    {

 

        try (CSVReader reader = new CSVReader(new FileReader("upload.csv"), ','); 

                     Connection connection = DBConnection.getConnection();)

        {

                String insertQuery = "Insert into txn_tbl (id,name,mob_no,address)";

                PreparedStatement pstmt = connection.prepareStatement(insertQuery);

                String[] rowData = null;

                int i = 0;

                while((rowData = reader.readNext()) != null)

                {

                    for (String data : rowData)

                    {

                            pstmt.setString((i % 3) + 1, data);

 

                            if (++i % 3 == 0)

                                    pstmt.addBatch();

 

                            if (i % 30 == 0)

                                    pstmt.executeBatch();

                    }

                }

                System.out.println("Data Successfully Uploaded");

        }

        catch (Exception e)

        {

                e.printStackTrace();

        }

 

    }

 

    private static void readCsvUsingLoad()

    {

        try (Connection connection = DBConnection.getConnection())

        {



                String loadQuery = "LOAD DATA LOCAL INFILE '" + "C:\\upload.csv" + "' INTO TABLE txn_tbl FIELDS TERMINATED BY ','" + " LINES TERMINATED BY '\n' (id,name,mob_no,address) ";

                System.out.println(loadQuery);

                Statement stmt = connection.createStatement();

                stmt.execute(loadQuery);

        }

        catch (Exception e)

        {

                e.printStackTrace();

        }

    }
}


 

