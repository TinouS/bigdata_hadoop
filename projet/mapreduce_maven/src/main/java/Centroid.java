
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ebayol
 */
public class Centroid {
    private String line;
    private ArrayList<Integer> values = new ArrayList<Integer>();
    
    public Centroid(String value, int col){
        String[] tokens = value.split(",");
        values.add(Integer.parseInt(tokens[col]));
        line = value;
    }
    
    public Centroid(int value){
        values.add(value);
        line = null;
    }
    
    String getLine (){
        return line;
    }
    
    int returnValue(int index){
        return values.get(index);
    }
}
