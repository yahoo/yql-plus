# YQL+ Source APIs

YQL+ source apis define the API for writing sources, transforms (pipes), and UDFs. It contains interfaces for tagging an implementation of each as well as annotations for communicating binding information to the engine.

## Annotations

* Source and Function Annotations <br>
<code> 
@Source <br>
@Exports
</code>

* Variable Annotations <br>
<code>
@Key <br>
@DefaultValue
</code>

* Operation Annotations <br>
<code>
@Query <br>
@Insert <br>
@Delete <br>
@Update
</code>

* Trace and Monitor Annotations <br>
<code>
@Emitter <br>
@Trace <br>
@TimeoutBudget
</code>

## Trace and Mornitor Apis

## YQL+ types