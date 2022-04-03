package dev.pabear.gitter.entity;

import lombok.Data;

@Data
public class Payload<T> {
  private Member from;
  private T content;
  private int incarnation;
}
