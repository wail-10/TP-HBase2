package ma.enset.tp_hbase2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App {
    public static final String TABLE_NAME = "Students";
    public static final String CF_INFOS = "infos";
    public static final String CF_GRADES = "grades";
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zookeeper");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "hbase-master:16000");

        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            System.out.println("Question 1: Creer une table Students avec deux familles colonnes: infos et grades");
            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_INFOS));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_GRADES));
            TableDescriptor tableDescriptor = builder.build();

            if (!admin.tableExists(tableName)){
                admin.createTable(tableDescriptor);
                System.out.println("La table est bien cree");
            } else {
                System.out.println("La table existe deja");
            }

            System.out.println("Question 2: Ajouter des donnees dans la table");
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes("student1"));
            put.addColumn(Bytes.toBytes(CF_INFOS), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
            put.addColumn(Bytes.toBytes(CF_INFOS), Bytes.toBytes("age"), Bytes.toBytes(20));
            put.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("B"));
            put.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(put);

            Put put2 = new Put(Bytes.toBytes("student2"));
            put2.addColumn(Bytes.toBytes(CF_INFOS), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
            put2.addColumn(Bytes.toBytes(CF_INFOS), Bytes.toBytes("age"), Bytes.toBytes(22));
            put2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A"));
            put2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(put2);
            System.out.println("Donnees ajoutees avec succes");

            // recuperer et affichez toutes les informations disponibles sur l'etudiant 1

            System.out.println("Question 3: Recuperer et affichez toutes les informations disponibles sur l'etudiant 1");
            Get get = new Get(Bytes.toBytes("student1"));
            Result result = table.get(get);
            byte[] name = result.getValue(Bytes.toBytes(CF_INFOS), Bytes.toBytes("name"));
            System.out.println("Nom: " + Bytes.toString(name));
            byte[] age = result.getValue(Bytes.toBytes(CF_INFOS), Bytes.toBytes("age"));
            System.out.println("Age: " + Bytes.toInt(age));
            byte[] mathGrade = result.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"));
            System.out.println("Note en math: " + Bytes.toString(mathGrade));
            byte[] scienceGrade = result.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"));
            System.out.println("Note en science: " + Bytes.toString(scienceGrade));
            // Changez l'âge de "Jane Smith" à "23" et mettez à jour sa note de math à "A+".
            System.out.println("Question 4: Mettre a jour les donnees de l'etudiant 2");
            Put put3 = new Put(Bytes.toBytes("student2"));
            put3.addColumn(Bytes.toBytes(CF_INFOS), Bytes.toBytes("age"), Bytes.toBytes(23));
            put3.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A+"));
            table.put(put3);
            System.out.println("Donnees mises a jour avec succes");
            // Supprimez l'étudiant avec la Row Key "student1" de la table Students.
            System.out.println("Question 5: Supprimer l'etudiant 1");
            Delete delete = new Delete(Bytes.toBytes("student1"));
            table.delete(delete);
            System.out.println("Donnees supprimees avec succes");
            //  Affichez toutes les informations pour tous les étudiants.
            System.out.println("Question 6: Recuperer et affichez toutes les informations disponibles sur tous les etudiants");
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Recuperation de toutes les donnees");
            for (Result res : scanner){
                System.out.println("Row key: " + Bytes.toString(res.getRow()));
                byte[] name2 = res.getValue(Bytes.toBytes(CF_INFOS), Bytes.toBytes("name"));
                System.out.println("Nom: " + Bytes.toString(name2));
                byte[] age2 = res.getValue(Bytes.toBytes(CF_INFOS), Bytes.toBytes("age"));
                System.out.println("Age: " + Bytes.toInt(age2));
                byte[] mathGrade2 = res.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"));
                System.out.println("Note en math: " + Bytes.toString(mathGrade2));
                byte[] scienceGrade2 = res.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"));
                System.out.println("Note en science: " + Bytes.toString(scienceGrade2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
