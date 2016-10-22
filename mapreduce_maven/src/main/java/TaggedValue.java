
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
public class TaggedValue implements Writable{
    
    private int type;//0 = town, 1 = region
    private String value;
    
    public TaggedValue(){
        
    }

    public TaggedValue(int type, String value){
        this.type = type;
        this.value = value;
    }
    
    public TaggedValue clone(){
        return new TaggedValue(type, value);
    }
    
    public int getType(){
        return type;
    }
    
    public String getValue(){
        return value;
    }
    
    public void write(DataOutput d) throws IOException {
        d.writeInt(type);
        d.writeChars(value);
    }

    public void readFields(DataInput di) throws IOException {
        type = di.readInt();
        value = di.readLine();
    }
    
    public String toString(){
        return value;
    }
}
