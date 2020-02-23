package code.ponfee.es.uss.res;

/**
 * USS data result
 * 
 * @author Ponfee
 */
public abstract class DataResult extends BaseResult {

    private static final long serialVersionUID = 723692947170070025L;

    // ---------------------------------data field
    private Long hitNum;
    private Long returnNum; // hits[]
    private Integer tookTime;

    public DataResult() {}

    public DataResult(BaseResult base) {
        super(base);
        if (base instanceof DataResult) {
            DataResult data = (DataResult) base;
            this.hitNum = data.hitNum;
            this.returnNum = data.returnNum;
            this.tookTime = data.tookTime;
        }
    }

    public Long getHitNum() {
        return hitNum;
    }

    public void setHitNum(Long hitNum) {
        this.hitNum = hitNum;
    }

    public Long getReturnNum() {
        return returnNum;
    }

    public void setReturnNum(Long returnNum) {
        this.returnNum = returnNum;
    }

    public Integer getTookTime() {
        return tookTime;
    }

    public void setTookTime(Integer tookTime) {
        this.tookTime = tookTime;
    }

}
