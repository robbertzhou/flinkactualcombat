package com.zy.chapter02;

public class MyOrder {
    public Long id;
    public String product;
    public int amount;

    public MyOrder(){

    }

    public MyOrder(Long id,String product,int amount){
        this.id = id;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "MyOrder{" +
                "id=" + id +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }
}
