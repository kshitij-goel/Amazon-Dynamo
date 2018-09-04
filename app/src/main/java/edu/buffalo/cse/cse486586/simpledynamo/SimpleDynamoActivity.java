package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

/*

References:

https://developer.android.com/reference/java/util/concurrent/BlockingQueue.html
https://developer.android.com/guide/topics/providers/content-providers.html
https://developer.android.com/reference/android/database/Cursor.html
https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
https://developer.android.com/reference/java/io/File.html
https://developer.android.com/reference/android/content/Context.html#deleteFile(java.lang.String)
https://developer.android.com/reference/android/view/View.OnClickListener.html
https://developer.android.com/reference/android/content/ContentValues.html
https://developer.android.com/reference/android/database/MatrixCursor.html
https://developer.android.com/reference/android/net/Uri.html
https://developer.android.com/reference/java/io/ObjectOutputStream.html
https://developer.android.com/reference/java/io/ObjectInputStream.html
https://developer.android.com/reference/java/util/Iterator.html
https://developer.android.com/reference/java/net/Socket.html
https://developer.android.com/reference/java/util/HashMap.html
https://developer.android.com/reference/java/util/PriorityQueue.html
https://stackoverflow.com/questions/1459656/how-to-get-the-current-time-in-yyyy-mm-dd-hhmisec-millisecond-format-in-java
http://tutorials.jenkov.com/java-internationalization/simpledateformat.html
https://stackoverflow.com/questions/12781273/what-are-the-date-formats-available-in-simpledateformat-class

*/

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		final Uri uriobject=buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");
		final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
		findViewById(R.id.button1).setOnClickListener(new OnTestClickListener(tv, getContentResolver()));

		final Button button2 = (Button) findViewById(R.id.button2);
		final Button button3 = (Button) findViewById(R.id.button3);

		button2.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {

				Cursor cur=getContentResolver().query(uriobject,null,"@",null,null);
				Log.d("test","Key in matcursor, key: "+cur.getColumnName(0)+" Value: "+cur.getColumnName(1));
				int i=0;
				int count=cur.getCount();
				tv.setText("");
				tv.append("Cursor matrix @ size: "+count+"\n");
				while(i<count){
					cur.moveToNext();
					String key=cur.getString(0);
					String value=cur.getString(1);
					Log.d("test","iteration,i: "+i+" Key: "+key+" Value: "+value);
					tv.append("Key: "+key+" Value: "+value+"\n");
					i++;
				}
				cur.close();
			}
		});
		button3.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {

				Cursor cur=getContentResolver().query(uriobject,null,"*",null,null);
				Log.d("test","Key in matcursor, key: "+cur.getColumnName(0)+" Value: "+cur.getColumnName(1));
				int i=0;
				int count=cur.getCount();
				tv.setText("");
				tv.append("Cursor matrix * size: "+count+"\n");
				while(i<count){
					cur.moveToNext();
					String key=cur.getString(0);
					String value=cur.getString(1);
					Log.d("test","iteration,i: "+i+" Key: "+key+" Value: "+value);
					tv.append("Key: "+key+" Value: "+value+"\n");
					i++;
				}
				cur.close();
			}
		});

	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

}