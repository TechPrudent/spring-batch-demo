package com.techprudent.batch;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="people")
public class People {
	
	@Id
	private Long rowId;
	
	private String orderId;
	private String orderDate;
	private String shipDate;
	private String customerId;
	private String customerName;
	private String segment;
	private String city;
	private String state;
	private String sales;
	private String quantity;
	private String discount;
	private String profit;
	
	
	public Long getRowId() {
		return rowId;
	}
	public void setRowId(Long rowId) {
		this.rowId = rowId;
	}
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getOrderDate() {
		return orderDate;
	}
	public void setOrderDate(String orderDate) {
		this.orderDate = orderDate;
	}
	public String getShipDate() {
		return shipDate;
	}
	public void setShipDate(String shipDate) {
		this.shipDate = shipDate;
	}
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public String getCustomerName() {
		return customerName;
	}
	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}
	public String getSegment() {
		return segment;
	}
	public void setSegment(String segment) {
		this.segment = segment;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getSales() {
		return sales;
	}
	public void setSales(String sales) {
		this.sales = sales;
	}
	public String getQuantity() {
		return quantity;
	}
	public void setQuantity(String quantity) {
		this.quantity = quantity;
	}
	public String getDiscount() {
		return discount;
	}
	public void setDiscount(String discount) {
		this.discount = discount;
	}
	public String getProfit() {
		return profit;
	}
	public void setProfit(String profit) {
		this.profit = profit;
	}
	@Override
	public String toString() {
		return "People [rowId=" + rowId + ", orderId=" + orderId + ", orderDate=" + orderDate + ", shipDate=" + shipDate
				+ ", customerId=" + customerId + ", customerName=" + customerName + ", segment=" + segment + ", city="
				+ city + ", state=" + state + ", sales=" + sales + ", quantity=" + quantity + ", discount=" + discount
				+ ", profit=" + profit + "]";
	}
		
	
	
	
}
