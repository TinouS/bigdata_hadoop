
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ebayol
 */
public class TaggedKey implements WritableComparable<TaggedKey>{
    private int type;//0 = town, 1 = region
    private String value;

    public TaggedKey(){
        
    }
    public TaggedKey(int type , String value){
        this.type=type;
        this.value=value;
        
    }
    public int getType(){
        return type;
    }
    
    public String getNaturalKey(){
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

    public int compareTo(TaggedKey o) {
        return (type < o.getType()? -1 : (type == o.getType()? 0 : 1));
    }
    
}
