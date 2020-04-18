package com.yahoo.yqlplus.engine.internal.compiler.streams;

/**
 * Created by daisyzhu on 4/17/20.
 */
public class PlanProgramCompileOptions {

    public static final PlanProgramCompileOptions DEFAULT_OPTIONS = new PlanProgramCompileOptions.PlanProgramOptionsBuilder().build();

    private boolean keepMergeSequential; //enable this option merge tables will be sequential

    private PlanProgramCompileOptions(PlanProgramOptionsBuilder builder) {
        this.keepMergeSequential = builder.keepMergeSequential;
    }

    public boolean isKeepMergeSequential() {
        return keepMergeSequential;
    }

    public static final class PlanProgramOptionsBuilder {
        private boolean keepMergeSequential;
        public PlanProgramOptionsBuilder keepMergeSequential(boolean keepMergeSequential) {
            this.keepMergeSequential = keepMergeSequential;
            return this;
        }

        public PlanProgramCompileOptions build() {
            return new PlanProgramCompileOptions(this);
        }
    }

    @Override
    public String toString() {
        return "PlanProgramCompileOptions{" +
                "keepMergeSequential=" + keepMergeSequential +
                '}';
    }
}
