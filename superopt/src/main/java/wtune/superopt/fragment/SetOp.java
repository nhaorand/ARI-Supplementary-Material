package wtune.superopt.fragment;

public interface SetOp extends Op{
  boolean deduplicated();

  void setDeduplicated(boolean flag);
}
