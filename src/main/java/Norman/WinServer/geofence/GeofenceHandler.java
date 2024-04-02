package Norman.WinServer.geofence;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class GeofenceHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        String data = (String) msg;

        // Process the data using geofencing logic
        String processedData = geofencing(data);

        // Print the processed data
        System.out.println("Processed data from GeofenceHandler: " + processedData);

        // Encode the processed data into ByteBuf
        ByteBuf encodedData = Unpooled.copiedBuffer(processedData.getBytes());

        // Pass the encoded data to ServerHandler
        ctx.fireChannelRead(encodedData);
    }


    private String geofencing(String data) {
        // Perform geofencing logic here
        // For example, manipulate the data
        data = "";
        return data;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Handle exceptions
        cause.printStackTrace();
        ctx.close();
    }
}
