/**
 *@name Consulta.java
 * 
 *@version 1.0 
 * 
 *@date 30-03-2017
 * 
 *@author EA7129
 * 
 *@copyright Cencosud. All rights reserved.
 */
package corp.cencosud.cuadratura.eomkpi3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
/**
 * @description 
 */
public class Consulta3 {
	
	private static final int DIFF_HOY_FECHA_INI = 16;
	private static final int DIFF_HOY_FECHA_FIN = 14;
	private static final int FORMATO_FECHA_0 = 0;
	private static final int FORMATO_FECHA_1 = 1;
	private static final int FORMATO_FECHA_3 = 3;
	private static final String RUTA_ENVIO = "C:/Share/Inbound/EOM_3";

	private static BufferedWriter bw;
	private static String path;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {
			
			

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
		}
	}
	
	private static void crearTxt(Map<String, String> mapArguments) {
		// TODO Auto-generated method stub
		Connection dbconnOracle = crearConexionOracle();
		File file1              = null;
		BufferedWriter bw       = null;
		PreparedStatement pstmt = null;
		StringBuffer sb         = null;
		String sFechaIni        = null;
		String sFechaFin        = null;
		
		
		Date now2 = new Date();
		SimpleDateFormat ft2 = new SimpleDateFormat ("dd/MM/YY hh:mm:ss");
		String currentDate2 = ft2.format(now2);
		info("Inicio Programa: " + currentDate2 + "\n");

		try {

			try {

				sFechaIni = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_INI);
				sFechaFin = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);
				//sFechaIni = "29-03-2017";
				//sFechaFin = "29-03-2017";
				info("sFechaIni: " + sFechaIni + "\n");
				info("sFechaFin: " + sFechaFin + "\n");
			}
			catch (Exception e) {

				info("error: " + e);
			}
			//file1                   = new File(path + "/" + sFechaIni + "_" + sFechaFin + ".txt");
			file1                   = new File(RUTA_ENVIO + "/" + sFechaIni + "_" + sFechaFin + ".txt");
			sb = new StringBuffer();
			
			
			
			//Version 3 Obtiene los Pick Up y Click and collect
			sb.append("SELECT PO.tc_purchase_orders_id   as N_Solicitud_Cliente ");
			sb.append(",POL.tc_po_line_id ");
			sb.append(",PO.created_dttm            as Fecha_Creacion_Solic ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",O.created_dttm             as Fecha_Creacion_Orden ");
			sb.append(",MIN(POL.comm_dlvr_dttm)    as FechaCompromiso ");
			sb.append(",MIN(POL.COMM_SHIP_DTTM)    as FechaSalidaCD ");
			sb.append(",TO_CHAR(MIN(POL.comm_dlvr_dttm), 'DD-MM-YYYY HH24:MI:SS') as FechaEntregaCliente ");
			sb.append(",POS.description            as Est_Orden ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.allocated_qty          as Cant_Desc_SKU ");
			sb.append(",IC.prod_type               as Depart ");
			sb.append(",PO.entry_code              as Local_Venta ");
			sb.append(",MAX(AO.A_DCNAME)           AS BodegaDesp ");
			sb.append(",NVL(MAX(ASND.ref_field_7), 'Sin Informacion')      AS Est_1  ");
			sb.append(",NVL(MAX(ASND.ref_field_9), 'Sin Informacion')      AS Est_Entrega ");
			sb.append(",MIN(ASND.ref_field_10)     AS Fecha_Entr_Cliente ");
			sb.append(",PO.customer_user_id        AS RutCliente ");
			sb.append(",PO.customer_firstname      AS NombreCliente ");
			sb.append(",PO.customer_lastname       AS ApellidoCliente ");
			sb.append(",POL.d_address_1            as Direccion_Cliente  ");
			sb.append(",POL.d_county               as Comuna ");
			sb.append(",POL.d_city                 as Ciudad ");
			sb.append(",POL.d_state_prov           as Region ");
			sb.append(",POLW.REF_FIELD_2           as Horario ");
			sb.append(",OT.order_type              as Tipo_Orden ");
			sb.append(",CASE   WHEN ao.a_allocation_type = 1 THEN 'VEV CD' WHEN ao.a_allocation_type = 0 AND MAX(AO.A_DCNAME) NOT IN ('012', '200')THEN 'VEV PD' ELSE  'CD DESPACHA' END    as Tipo_Venta ");
			//sb.append(",null as GDD ");//--
			//sb.append(",null as HojaRuta ");//--
			sb.append(",pol.comm_dlvr_dttm         as fecha_compromiso_2 ");
			sb.append(",o.O_FACILITY_ALIAS_ID ");
			sb.append(",CASE  WHEN POL.d_state_prov in (13,50) THEN 'RM' ELSE 'REGIONES'  END AS RegionEntrega ");
			sb.append(",CASE  WHEN OT.order_type_id = 2 OR OT.order_type_id = 3 THEN 'POS'  ELSE OT.order_type END AS TIPO_DE_ORDEN ");
			sb.append(",CASE ");
			sb.append(" WHEN ");
			sb.append(" MAX(ASND.ref_field_7) in ('Entregado','Entrega ','Entrega parcial', 'Recogido', 'Recogido parcial')  ");
			sb.append(" AND (TO_NUMBER(TO_CHAR((MIN(POL.comm_dlvr_dttm)),'YYYYMMDD')) - TO_NUMBER(to_char(to_timestamp((MIN(ASND.ref_field_10)),'YYYY-MM-DD HH24:MI:SS,FF3'),'YYYYMMDD')))  < -0.001 then 'ATRASADO' ");
			sb.append(" WHEN MAX(ASND.ref_field_7) in ('Entregado','Entrega ','Entrega parcial', 'Recogido', 'Recogido parcial')   ");
			sb.append(" AND (TO_NUMBER(TO_CHAR((MIN(POL.comm_dlvr_dttm)),'YYYYMMDD')) - TO_NUMBER(to_char(to_timestamp((MIN(ASND.ref_field_10)),'YYYY-MM-DD HH24:MI:SS,FF3'),'YYYYMMDD')))  <= 0 then 'EN FECHA' ");
			sb.append(" WHEN MAX(ASND.ref_field_7) in ('Entregado','Entrega ','Entrega parcial', 'Recogido', 'Recogido parcial')  ");
			sb.append(" AND (TO_NUMBER(TO_CHAR((MIN(POL.comm_dlvr_dttm)),'YYYYMMDD')) - TO_NUMBER(to_char(to_timestamp((MIN(ASND.ref_field_10)),'YYYY-MM-DD HH24:MI:SS,FF3'),'YYYYMMDD')))  >0 then 'ADELANTADO' ");
			sb.append(" else ");
			sb.append(" NVL(MAX(ASND.ref_field_9), 'Sin Informacion') ");
			sb.append("end as cumplimiento ");
			sb.append(",POL.D_POSTAL_CODE ");
			sb.append(",PO.EXT_PURCHASE_ORDERS_ID ");
			sb.append(",POL.TOTAL_MONETARY_VALUE ");
			sb.append(",POL.UNIT_MONETARY_VALUE ");
			sb.append(",PO.TC_PURCHASE_ORDERS_ID ");
			sb.append(",PO.PURCHASE_ORDERS_TYPE ");
			sb.append(",PO.PARENT_PURCHASE_ORDERS_ID ");
			sb.append(",nvl(POL.D_EMAIL, o.bill_to_email) as D_EMAIL ");
			sb.append(",po.on_hold ");
			sb.append(",po.payment_status ");
			sb.append(",POL.order_fulfillment_option ");
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id = po.purchase_orders_id AND POL.Purchase_Orders_Line_Status <> 940 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos  ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2   ON pos2.purchase_orders_status = pol.purchase_orders_line_status  ");
			sb.append("INNER JOIN CA14.a_orderinventoryallocation ao ON ao.a_orderlineid = pol.purchase_orders_line_item_id AND AO.A_DCNAME NOT IN ('012', '200') AND ao.a_skuid =  pol.sku_id  ");
			sb.append("INNER JOIN CA14.order_type ot  ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI  ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0   ");
			sb.append("left JOIN CA14.orders o ON o.purchase_order_id =  po.purchase_orders_id  AND o.Order_ID = OLI.Order_ID AND o.IS_CANCELLED = 0 ");
			sb.append("LEFT JOIN CA14.ASN_DETAIL ASND ON asnd.ORDER_LINE_ITEM_ID = OLI.LINE_ITEM_ID  AND asnd.order_id           = OLI.Order_Id AND asnd.tc_order_line_id   = OLI.Tc_Order_Line_Id AND ASND.ORDER_LINE_ITEM_ID = OLI.LINE_ITEM_ID  ");
			sb.append("LEFT JOIN CA14.PO_LINE_WMPROCESSINFO POLW      ON POLW.PURCHASE_ORDERS_LINE_ITEM_ID = pol.PURCHASE_ORDERS_LINE_ITEM_ID  ");

			sb.append("WHERE ot.order_type_id IN (2, 3, 6, 12, 14, 22, 42, 82) AND PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND pol.comm_dlvr_dttm >= to_date('");
			sb.append(sFechaIni);
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND pol.comm_dlvr_dttm <= to_date('");
			sb.append(sFechaFin);
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append(" AND O.tc_order_id is not null ");
			sb.append(" AND ((o.o_facility_alias_id  NOT IN ('012', '200') AND POL.Purchase_Orders_Line_Status > 400) ");
			sb.append(" OR (o.o_facility_alias_id  IN ('012', '200') AND POL.Purchase_Orders_Line_Status <= 400))  ");
			sb.append("GROUP BY ");
			sb.append("po.tc_purchase_orders_id, po.created_dttm, o.tc_order_id, o.created_dttm, pol.must_dlvr_dttm  ");
			sb.append(",pos.description, pol.sku, pol.allocated_qty, ic.prod_type, po.entry_code, pol.d_address_1 ");
			sb.append(",pol.d_county, pol.d_city, pol.d_state_prov, ot.order_type, POLW.REF_FIELD_2, po.customer_user_id ");
			sb.append(",po.customer_firstname, po.customer_lastname, pos2.description ");
			sb.append(",pol.tc_po_line_id, pol.comm_dlvr_dttm, o.O_FACILITY_ALIAS_ID ");
			sb.append(",ao.a_allocation_type,POL.D_POSTAL_CODE ,PO.EXT_PURCHASE_ORDERS_ID,POL.TOTAL_MONETARY_VALUE,POL.UNIT_MONETARY_VALUE,PO.TC_PURCHASE_ORDERS_ID,PO.PURCHASE_ORDERS_TYPE,PO.PARENT_PURCHASE_ORDERS_ID,POL.D_EMAIL,po.on_hold,po.payment_status,OT.order_type_id,o.bill_to_email,POL.order_fulfillment_option ");
			sb.append(" UNION ");
			
			sb.append("SELECT   PO.tc_purchase_orders_id   as N_Solicitud_Cliente ");
			sb.append(",POL.tc_po_line_id ");
			sb.append(",PO.created_dttm            as Fecha_Creacion_Solic ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",O.created_dttm             as Fecha_Creacion_Orden ");
			sb.append(",MIN(POL.comm_dlvr_dttm)    as FechaCompromiso ");
			sb.append(",MIN(POL.COMM_SHIP_DTTM)    as FechaSalidaCD  ");
			sb.append(",TO_CHAR(MIN(POL.comm_dlvr_dttm), 'DD-MM-YYYY HH24:MI:SS') as FechaEntregaCliente ");
			sb.append(",POS.description            as Est_Orden ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.allocated_qty          as Cant_Desc_SKU ");
			sb.append(",IC.prod_type               as Depart ");
			sb.append(",PO.entry_code              as Local_Venta ");
			sb.append(",MAX(AO.A_DCNAME)           AS BodegaDesp ");
			sb.append(",NVL(MAX(ASND.ref_field_7), 'Sin Informacion')      AS Est_1 ");
			sb.append(",NVL(MAX(ASND.ref_field_9), 'Sin Informacion')      AS Est_Entrega ");
			sb.append(",MIN(ASND.ref_field_10)     AS Fecha_Entr_Cliente  ");
			sb.append(",PO.customer_user_id        AS RutCliente ");
			sb.append(",PO.customer_firstname      AS NombreCliente ");
			sb.append(",PO.customer_lastname       AS ApellidoCliente ");
			sb.append(",POL.d_address_1            as Direccion_Cliente");
			sb.append(",POL.d_county               as Comuna ");
			sb.append(",POL.d_city                 as Ciudad ");
			sb.append(",POL.d_state_prov           as Region ");
			sb.append(",POLW.REF_FIELD_2           as Horario ");
			sb.append(",OT.order_type              as Tipo_Orden  ");
			sb.append(",CASE  WHEN ao.a_allocation_type = 1 THEN 'VEV CD' WHEN ao.a_allocation_type = 0 AND MAX(AO.A_DCNAME) NOT IN ('012', '200')THEN 'VEV PD' ELSE  'CD DESPACHA' END    as Tipo_Venta ");
			sb.append(",pol.comm_dlvr_dttm         as fecha_compromiso_2 ");
			sb.append(",o.O_FACILITY_ALIAS_ID ");
			sb.append(",CASE WHEN POL.d_state_prov in (13,50) THEN 'RM' ELSE 'REGIONES' END AS RegionEntrega ");
			sb.append(",CASE WHEN OT.order_type_id = 2 OR OT.order_type_id = 3 THEN 'POS' ELSE OT.order_type END AS TIPO_DE_ORDEN ");
			sb.append(",CASE  ");
			sb.append("WHEN ");
			sb.append(" MAX(ASND.ref_field_7) in ('Entregado','Entrega ','Entrega parcial', 'Recogido', 'Recogido parcial') ");
			sb.append(" AND (TO_NUMBER(TO_CHAR((MIN(POL.comm_dlvr_dttm)),'YYYYMMDD')) - TO_NUMBER(to_char(to_timestamp((MIN(ASND.ref_field_10)),'YYYY-MM-DD HH24:MI:SS,FF3'),'YYYYMMDD')))  < -0.001 then 'ATRASADO' ");
			sb.append(" WHEN MAX(ASND.ref_field_7) in ('Entregado','Entrega ','Entrega parcial', 'Recogido', 'Recogido parcial')   ");
			sb.append(" AND (TO_NUMBER(TO_CHAR((MIN(POL.comm_dlvr_dttm)),'YYYYMMDD')) - TO_NUMBER(to_char(to_timestamp((MIN(ASND.ref_field_10)),'YYYY-MM-DD HH24:MI:SS,FF3'),'YYYYMMDD')))  <= 0 then 'EN FECHA' ");
			sb.append(" WHEN  MAX(ASND.ref_field_7) in ('Entregado','Entrega ','Entrega parcial', 'Recogido', 'Recogido parcial')  ");
			sb.append(" AND (TO_NUMBER(TO_CHAR((MIN(POL.comm_dlvr_dttm)),'YYYYMMDD')) - TO_NUMBER(to_char(to_timestamp((MIN(ASND.ref_field_10)),'YYYY-MM-DD HH24:MI:SS,FF3'),'YYYYMMDD')))  >0 then 'ADELANTADO' ");
			sb.append("else ");
			sb.append("NVL(MAX(ASND.ref_field_9), 'Sin Informacion') ");
			sb.append("end as cumplimiento ");
			sb.append(",POL.D_POSTAL_CODE ");
			sb.append(",PO.EXT_PURCHASE_ORDERS_ID ");
			sb.append(",POL.TOTAL_MONETARY_VALUE ");
			sb.append(",POL.UNIT_MONETARY_VALUE           ");
			sb.append(",po.TC_PURCHASE_ORDERS_ID ");
			sb.append(",po.PURCHASE_ORDERS_TYPE  ");
			sb.append(",po.PARENT_PURCHASE_ORDERS_ID ");
			sb.append(",nvl(POL.D_EMAIL, o.bill_to_email) as D_EMAIL ");
			sb.append(",po.on_hold ");
			sb.append(",po.payment_status ");
			sb.append(",POL.order_fulfillment_option ");
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id = po.purchase_orders_id AND POL.Purchase_Orders_Line_Status <> 940 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos   ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.a_orderinventoryallocation ao  ON ao.a_orderlineid = pol.purchase_orders_line_item_id AND ao.a_skuid =  pol.sku_id ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category  ");
			sb.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb.append("LEFT JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID  AND Oli.Is_Cancelled = 0 ");
			sb.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID  AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb.append("LEFT JOIN CA14.ASN_DETAIL ASND ON asnd.ORDER_LINE_ITEM_ID = OLI.LINE_ITEM_ID AND asnd.order_id           = OLI.Order_Id AND asnd.tc_order_line_id   = OLI.Tc_Order_Line_Id AND ASND.ORDER_LINE_ITEM_ID = OLI.LINE_ITEM_ID ");
            sb.append("LEFT JOIN CA14.PO_LINE_WMPROCESSINFO POLW    ON POLW.PURCHASE_ORDERS_LINE_ITEM_ID = pol.PURCHASE_ORDERS_LINE_ITEM_ID ");
            sb.append("LEFT JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
            sb.append("WHERE ot.order_type_id in (2, 3, 6, 12, 14, 22, 42, 82)  AND PO.is_purchase_orders_confirmed = '1'   AND nvl(o.D_FACILITY_ALIAS_ID, 0) = 0 ");
			sb.append("AND pol.comm_dlvr_dttm >= to_date('");
			sb.append(sFechaIni);
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND pol.comm_dlvr_dttm <= to_date('");
			sb.append(sFechaFin);
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			
			sb.append("  AND O.tc_order_id NOT IN ( SELECT O.tc_order_id ");
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL    ON pol.purchase_orders_id = po.purchase_orders_id AND POL.Purchase_Orders_Line_Status <> 940 ");
			sb.append("INNER JOIN CA14.a_orderinventoryallocation ao    ON ao.a_orderlineid = pol.purchase_orders_line_item_id AND AO.A_DCNAME not in ('012', '200') AND ao.a_skuid =  pol.sku_id ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb.append("INNER JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 AND o.o_facility_alias_id  IN ('012', '200', '400') ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");

			sb.append("WHERE ot.order_type_id in (2, 3, 6, 12, 14, 22, 42, 82) ");
			sb.append(" AND PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND pol.comm_dlvr_dttm >= to_date('");
			sb.append(sFechaIni);
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND pol.comm_dlvr_dttm <= to_date('");
			sb.append(sFechaFin);
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append(") ");
			
			sb.append(" GROUP BY ");
			sb.append("po.tc_purchase_orders_id, po.created_dttm, o.tc_order_id, o.created_dttm, pol.must_dlvr_dttm ");
			sb.append(",pos.description, pol.sku, pol.allocated_qty, ic.prod_type, po.entry_code, pol.d_address_1 ");
			sb.append(",pol.d_county, pol.d_city, pol.d_state_prov, ot.order_type, POLW.REF_FIELD_2, po.customer_user_id ");
			sb.append(",po.customer_firstname, po.customer_lastname, pos2.description ");
			sb.append(",pol.tc_po_line_id, pol.comm_dlvr_dttm, o.O_FACILITY_ALIAS_ID,ao.a_allocation_type,POL.D_POSTAL_CODE,PO.EXT_PURCHASE_ORDERS_ID,POL.TOTAL_MONETARY_VALUE,POL.UNIT_MONETARY_VALUE     ,PO.TC_PURCHASE_ORDERS_ID,PO.PURCHASE_ORDERS_TYPE,PO.PARENT_PURCHASE_ORDERS_ID,POL.D_EMAIL,po.on_hold,po.payment_status,OT.order_type_id,o.bill_to_email,POL.order_fulfillment_option ");
			


			
			info("Query : " + sb + "\n");
			
			pstmt = dbconnOracle.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			bw = new BufferedWriter(new FileWriter(file1));
			bw.write("N_SOLICITUD_CLIENTE;");
			bw.write("TC_PO_LINE_ID;");
			bw.write("FECHA_CREACION_SOLIC;");
			bw.write("N_ORDEN_DISTRIBU;");
			bw.write("FECHA_CREACION_ORDEN;");
			bw.write("FECHACOMPROMISO;");
			bw.write("FECHASALIDACD;");
			bw.write("FECHAENTREGACLIENTE;");
			bw.write("EST_ORDEN;");
			bw.write("ESTADO_DE_LINEA;");
			bw.write("SKU;");
			bw.write("CANT_DESC_SKU;");
			bw.write("DEPART;");
			bw.write("LOCAL_VENTA;");
			bw.write("BODEGADESP;");
			bw.write("EST_1;");
			bw.write("EST_ENTREGA;");
			bw.write("FECHA_ENTR_CLIENTE;");
			bw.write("RUTCLIENTE;");
			bw.write("NOMBRECLIENTE;");
			bw.write("APELLIDOCLIENTE;");
			bw.write("DIRECCION_CLIENTE;");
			bw.write("COMUNA;");
			bw.write("CIUDAD;");
			bw.write("REGION;");
			bw.write("HORARIO;");
			bw.write("TIPO_ORDEN;");
			bw.write("TIPO_VENTA;");
			//bw.write("GDD;");
			//bw.write("HOJARUTA;");
			bw.write("FECHA_COMPROMISO_2;");
			bw.write("O_FACILITY_ALIAS_ID;");
			bw.write("REGIONENTREGA;");
			bw.write("TIPO_DE_ORDEN;");
			bw.write("CUMPLIMIENTO;");
			bw.write("D_POSTAL_CODE;");
			bw.write("EXT_PURCHASE_ORDERS_ID;");
			bw.write("TOTAL_MONETARY_VALUE;");
			bw.write("UNIT_MONETARY_VALUE;");
			bw.write("TC_PURCHASE_ORDERS_ID;");
			bw.write("PURCHASE_ORDERS_TYPE;");
			bw.write("PARENT_PURCHASE_ORDERS_ID;");
			bw.write("D_EMAIL;");
			bw.write("ON_HOLD;");
			bw.write("PAYMENT_STATUS;");
			bw.write("ORDER_FULFILLMENT_OPTION\n");
			sb = new StringBuffer();
			
			while (rs.next()){

				bw.write(rs.getString("N_SOLICITUD_CLIENTE") + ";");
				bw.write(rs.getString("TC_PO_LINE_ID") + ";");
				bw.write(formatDate(rs.getTimestamp("FECHA_CREACION_SOLIC"), FORMATO_FECHA_0) + ";");
				bw.write(rs.getString("N_ORDEN_DISTRIBU") + ";");
				bw.write(formatDate(rs.getTimestamp("FECHA_CREACION_ORDEN"), FORMATO_FECHA_0) + ";");
				bw.write(formatDate(rs.getTimestamp("FECHACOMPROMISO"), FORMATO_FECHA_1) + ";");
				bw.write(formatDate(rs.getTimestamp("FECHASALIDACD"), FORMATO_FECHA_1) + ";");
				bw.write(rs.getString("FECHAENTREGACLIENTE") + ";");
				bw.write(rs.getString("EST_ORDEN") + ";");
				bw.write(rs.getString("ESTADO_DE_LINEA") + ";");
				bw.write(rs.getString("SKU") + ";");
				bw.write(rs.getString("CANT_DESC_SKU") + ";");
				bw.write(rs.getString("DEPART") + ";");
				bw.write(rs.getString("LOCAL_VENTA") + ";");
				bw.write(rs.getString("BODEGADESP") + ";");
				bw.write(rs.getString("EST_1") + ";");
				bw.write(rs.getString("EST_ENTREGA") + ";");
				bw.write(formatDate(rs.getTimestamp("FECHA_ENTR_CLIENTE"), FORMATO_FECHA_3) + ";");
				bw.write(rs.getString("RUTCLIENTE") + ";");
				bw.write(rs.getString("NOMBRECLIENTE") + ";");
				bw.write(rs.getString("APELLIDOCLIENTE") + ";");
				bw.write(rs.getString("DIRECCION_CLIENTE") + ";");
				bw.write(rs.getString("COMUNA") + ";");
				bw.write(rs.getString("CIUDAD") + ";");
				bw.write(rs.getString("REGION") + ";");
				bw.write(rs.getString("HORARIO") + ";");
				bw.write(rs.getString("TIPO_ORDEN") + ";");
				bw.write(rs.getString("TIPO_VENTA") + ";");
				//bw.write(rs.getString("GDD") + ";");
				//bw.write(rs.getString("HOJARUTA") + ";");
				bw.write(formatDate(rs.getTimestamp("FECHA_COMPROMISO_2"), FORMATO_FECHA_1) + ";");
				bw.write(rs.getString("O_FACILITY_ALIAS_ID") + ";");
				bw.write(rs.getString("REGIONENTREGA") + ";");
				bw.write(rs.getString("TIPO_DE_ORDEN") + ";");
				bw.write(rs.getString("CUMPLIMIENTO") + ";");
				bw.write(rs.getString("D_POSTAL_CODE") + ";");
				bw.write(rs.getString("EXT_PURCHASE_ORDERS_ID") + ";");
				bw.write(rs.getString("TOTAL_MONETARY_VALUE") + ";");
				bw.write(rs.getString("UNIT_MONETARY_VALUE") + ";");
				bw.write(rs.getString("TC_PURCHASE_ORDERS_ID") + ";");
				bw.write(rs.getString("PURCHASE_ORDERS_TYPE") + ";");
				bw.write(rs.getString("PARENT_PURCHASE_ORDERS_ID") + ";");
				bw.write(rs.getString("D_EMAIL") + ";");
				bw.write(rs.getString("ON_HOLD") + ";");
				bw.write(rs.getString("PAYMENT_STATUS") + ";");
				bw.write(rs.getString("ORDER_FULFILLMENT_OPTION") + "\n");
			}
			bw.write(sb.toString());
			info("Archivos creados." + "\n");
		}
		catch (Exception e) {

			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnOracle, pstmt, bw);
		}
		info("Fin Programa: " + currentDate2 + "\n");
	}

	/**
	 * Metodo de conexion para MEOMCLP 
	 * 
	 * @return void,  no tiene valor de retorno
	 */
	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			//Shareplex
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");
			
			//El servidor g500603sv0zt corresponde a Produccion. Por el momento
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP","ca14","Manhattan1234");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	}


	/**
	 * Metodo que cierra la conexion, Procedimintos,  BufferedWriter
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param PreparedStatement, Objeto que representa una instrucción SQL precompilada. 
	 * @return retorna
	 * 
	 */
	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}


	/**
	 * Metodo que muestra informacion 
	 * 
	 * @param String, texto a mostra
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:" + e.getMessage());
		}
	}


	/**
	 * Metodo que resta dias 
	 * 
	 * @param String, dia que se resta
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "dd-MM-yyyy";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}

	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 */
	private static String formatDate(Date fecha, int iOptFormat) {

		String sFormatedDate = null;
		String sFormat = null;

		try {

			SimpleDateFormat df = null;

			switch (iOptFormat) {

			case 0:
				sFormat = "dd/MM/yy HH:mm:ss,SSS";
				break;
			case 1:
				sFormat = "dd/MM/yy";
				break;
			case 2:
				sFormat = "dd/MM/yy HH:mm:ss";
				break;
			case 3:
				sFormat = "yyyy-MM-dd HH:mm:ss,SSS";
				break;
			}
			df = new SimpleDateFormat(sFormat);
			sFormatedDate = df.format(fecha != null ? fecha:new Date(0));

			if (iOptFormat == 0 && sFormatedDate != null) {

				sFormatedDate = sFormatedDate + "000000";
			}
		}
		catch (Exception e) {

			info("[formatDate]Exception:"+e.getMessage());
		}
		return sFormatedDate;
	}

}
