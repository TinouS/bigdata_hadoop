
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ebayol
 */
public class VilleWritable implements Writable{

    private float population;
    private String info;
    public VilleWritable(){
        
    }
    
    public VilleWritable(float pop, String i){
        population = pop;
        info = i;
    }
    
    public float getPopulation(){
        return population;
    }
    
    public String getInfo(){
        return info;
    }
    
    public void write(DataOutput d) throws IOException {
        d.writeFloat(population);
        d.writeChars(info);
    }

    public void readFields(DataInput di) throws IOException {
        population = di.readFloat();
        info = di.readLine();
    }
    
}
